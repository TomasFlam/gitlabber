from typing import Optional, List
import logging
import os
import os.path
import sys
import subprocess
import git
from anytree import Node
from .progress import ProgressBar
import concurrent.futures
import fnmatch

log = logging.getLogger(__name__)

progress = ProgressBar('* syncing projects')


class GitAction:
    def __init__(self, 
                 node: Node,
                 path: str,
                 recursive: bool = False,
                 use_fetch: bool = False,
                 hide_token: bool = False,
                 git_options: Optional[str] = None) -> None:
        self.node = node
        self.path = path
        self.recursive = recursive
        self.use_fetch = use_fetch
        self.hide_token = hide_token
        self.git_options = git_options


def sync_tree(root: Node, 
              dest: str, 
              concurrency: int = 1,
              disable_progress: bool = False,
              recursive: bool = False,
              use_fetch: bool = False,
              hide_token: bool = False,
              git_options: Optional[str] = None) -> None:
    """
    Synchronizes the git repositories in the tree structure
    
    Args:
        root: Root node of the tree
        dest: Destination directory
        concurrency: Number of concurrent git operations
        disable_progress: Whether to disable progress reporting
        recursive: Whether to clone recursively
        use_fetch: Whether to use git fetch instead of pull
        hide_token: Whether to hide token in URLs
        git_options: Additional git options as comma-separated string
    """
    if not disable_progress:
        progress.init_progress(len(root.leaves))

    actions = get_git_actions(root, dest, recursive, use_fetch, hide_token)

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        executor.map(clone_or_pull_project, actions)
    
    elapsed = progress.finish_progress()
    log.debug("Syncing projects took [%s]", elapsed)


def get_git_actions(root, dest, recursive, use_fetch, hide_token):
    actions = []
    for child in root.children:
        path = f"{dest}{child.root_path}"
        if not os.path.exists(path):
            os.makedirs(path)
        if child.is_leaf:
            actions.append(GitAction(child, path, recursive, use_fetch, hide_token))            
        if not child.is_leaf:
            actions.extend(get_git_actions(child, dest, recursive, use_fetch, hide_token))
    return actions


def is_git_repo(path: str) -> bool:
    try:
        _ = git.Repo(path).git_dir
        return True
    except git.InvalidGitRepositoryError:
        return False


def list_branches_matching_patterns(repo: git.Repo, patterns: List[str]) -> List[str]:
    """
    Lists branches in the repository that match any of the provided patterns.
    """
    expanded_branches = []
    prefix = 'origin/'
    for ref in repo.references:
        if not ref.name.startswith(prefix):
            continue
        branch_name = ref.name[len(prefix):]
        for pattern in patterns:
            if fnmatch.fnmatch(branch_name, pattern):
                expanded_branches.append(branch_name)
                break
    return expanded_branches


def create_worktree(repo: git.Repo,  path: str, branch: str) -> None:
    """
    Creates a worktree for the specified branch at the given path.

    Args:
        repo: The git repository object.
        branch: The branch name to create a worktree for.
    """
    try:
        repo.git.worktree('add', path, branch)
    except Exception as e:
        log.error("Failed to add worktree for branch %s: %s", branch, e)


def update_worktree(path: str) -> None:
    """
    Updates the worktree at the specified path.

    Args:
        path: The path to the worktree.
    """
    try:
        git.Repo(path).git.pull()
    except Exception as e:
        log.error("Failed to update worktree at %s: %s", path, e)


def create_or_update_worktrees(repo: git.Repo, branches: List[str]) -> None:
    """
    Creates or updates worktrees for the specified branches in the repository.

    Args:
        repo: The git repository object.
        branches: List of branch names to create or update worktrees for.
    """
    for branch in branches:
        if branch == git.HEAD.name:
            continue

        path = os.path.join(repo.working_tree_dir, os.path.pardir, branch.replace('/', '-'))
        if os.path.exists(path):
            update_worktree(path)
        else:
            create_worktree(repo, path, branch)


def create_or_update_protected_branch_worktrees(repo: git.Repo, action: GitAction) -> None:
    """
    Adds worktrees for all branches that match the protected branch patterns.

    Args:
        repo: The git repository object.
        protected_branches: List of protected branch names or patterns.
    """
    if not hasattr(action.node, 'protected_branches') or not action.node.protected_branches:
        return
    branches = list_branches_matching_patterns(repo, action.node.protected_branches)
    create_or_update_worktrees(repo, branches)


def clone_or_pull_project(action: GitAction) -> None:
    if is_git_repo(action.path):
        '''
        Update existing project
        '''
        log.debug("updating existing project %s", action.path)
        progress.show_progress(action.node.name, 'pull')
        
        try:
            repo = git.Repo(action.path)
            if not action.use_fetch:
                repo.remotes.origin.pull()
            else:
                repo.remotes.origin.fetch()
            if action.recursive: 
                repo.submodule_update(recursive=True)
        except KeyboardInterrupt:
            log.fatal("User interrupted")
            sys.exit(0)
        except Exception as e:
            log.error("Error pulling project %s: %s", action.path, str(e), exc_info=True)
        else:
            create_or_update_protected_branch_worktrees(repo, action)
    else:
        '''
        Clone new project
        '''
        if action.node.type != "project":
            log.debug("Skipping clone of node with type [%s] (empty subgroup/group)", action.node.type)
            return
        log.debug("cloning new project %s", action.path)
        progress.show_progress(action.node.name, 'clone')
        multi_options: List[str] = []
        if action.recursive:
            multi_options.append('--recursive')
        if action.use_fetch:
            multi_options.append('--mirror')
        if action.git_options:
            multi_options += action.git_options.split(',')
        try:
            repo = git.Repo.clone_from(action.node.url, action.path, multi_options=multi_options)
        except KeyboardInterrupt:
            log.fatal("User interrupted")
            sys.exit(0)
        except Exception as e:
            log.error("Error cloning project %s: %s", action.path, str(e), exc_info=True)
        else:
            create_or_update_protected_branch_worktrees(repo, action)
