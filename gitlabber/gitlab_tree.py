from typing import List, Optional, Union, Any, Dict, Iterator
from gitlab import Gitlab
from gitlab.exceptions import GitlabGetError, GitlabListError, GitlabAuthenticationError, GitlabHttpError
from gitlab.v4.objects import Group, Project, User
from anytree import Node, RenderTree
from anytree.exporter import DictExporter, JsonExporter
from anytree.importer import DictImporter
from .git import sync_tree
from .format import PrintFormat
from .method import CloneMethod
from .naming import FolderNaming
from .progress import ProgressBar
from .auth import AuthProvider, TokenAuthProvider
import yaml
import globre
import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

class GitlabTreeError(Exception):
    """Base exception for GitlabTree errors."""
    pass

class GitlabTree:
    def __init__(self, 
                 url: str,
                 token: str,
                 method: CloneMethod,
                 naming: Optional[FolderNaming] = None,
                 archived: Optional[bool] = None,
                 includes: Optional[List[str]] = None,
                 excludes: Optional[List[str]] = None,
                 in_file: Optional[str] = None,
                 concurrency: int = 1,
                 recursive: bool = False,
                 disable_progress: bool = False,
                 include_shared: bool = True,
                 use_fetch: bool = False,
                 hide_token: bool = False,
                 user_projects: bool = False,
                 group_search: Optional[str] = None,
                 git_options: Optional[str] = None,
                 protected_branches: bool = False,
                 auth_provider: Optional[AuthProvider] = None) -> None:
        """Initialize GitlabTree.
        
        Args:
            url: GitLab instance URL
            token: Personal access token
            method: Clone method (SSH or HTTP)
            naming: Folder naming strategy
            archived: Whether to include archived projects
            includes: List of glob patterns to include
            excludes: List of glob patterns to exclude
            in_file: YAML file to load tree from
            concurrency: Number of concurrent git operations
            recursive: Whether to clone recursively
            disable_progress: Whether to disable progress bar
            include_shared: Whether to include shared projects
            use_fetch: Whether to use git fetch instead of pull
            hide_token: Whether to hide token in URLs
            user_projects: Whether to fetch only user projects
            group_search: Search term for filtering groups
            git_options: Additional git options as CSV string
            auth_provider: Authentication provider (defaults to TokenAuthProvider)
            
        Raises:
            GitlabTreeError: If initialization fails
        """
        self.includes = includes or []
        self.excludes = excludes or []
        self.url = url
        self.root = Node("", root_path="", url=url, type="root")
        
        # Use provided auth provider or default to token-based auth
        self.auth_provider = auth_provider or TokenAuthProvider(token)
        
        try:
            self.gitlab = Gitlab(url, private_token=token,
                               ssl_verify=GitlabTree.get_ca_path())
            # Authenticate using the provider
            self.auth_provider.authenticate(self.gitlab)
        except GitlabAuthenticationError as e:
            raise GitlabTreeError(f"Failed to authenticate with GitLab: {str(e)}")
        except Exception as e:
            raise GitlabTreeError(f"Failed to initialize GitLab client: {str(e)}")
            
        self.method = method
        self.naming = naming
        self.archived = archived
        self.in_file = in_file
        self.concurrency = concurrency
        self.recursive = recursive
        self.disable_progress = disable_progress
        self.progress = ProgressBar('* loading tree', disable_progress)
        self.token = token
        self.include_shared = include_shared
        self.use_fetch = use_fetch
        self.hide_token = hide_token
        self.user_projects = user_projects
        self.group_search = group_search
        self.git_options = git_options
        self.protected_branches = protected_branches

    @staticmethod
    def get_ca_path() -> Union[str, bool]:
        """Returns REQUESTS_CA_BUNDLE, CURL_CA_BUNDLE, or True"""
        return next(item for item in [os.getenv('REQUESTS_CA_BUNDLE', None), 
                                    os.getenv('CURL_CA_BUNDLE', None), 
                                    True]
                   if item is not None)

    def is_included(self, node: Node) -> bool:
        """Check if a node should be included based on include patterns.
        
        Args:
            node: Node to check
            
        Returns:
            True if node should be included, False otherwise
        """
        if not self.includes:
            return True
            
        for include in self.includes:
            log.debug("Checking requested include: %s with path: %s, match %s",
                     include, node.root_path, globre.match(include, node.root_path))
            if globre.match(include, node.root_path):
                return True
        return False

    def is_excluded(self, node: Node) -> bool:
        """Check if a node should be excluded based on exclude patterns.
        
        Args:
            node: Node to check
            
        Returns:
            True if node should be excluded, False otherwise
        """
        if not self.excludes:
            return False
            
        for exclude in self.excludes:
            log.debug("Checking requested exclude: %s with path: %s, match %s",
                     exclude, node.root_path, globre.match(exclude, node.root_path))
            if globre.match(exclude, node.root_path):
                return True
        return False

    def filter_tree(self, parent: Node) -> None:
        """Filter the tree based on include/exclude patterns.
        
        Args:
            parent: Parent node to filter
        """
        for child in parent.children:
            if not child.is_leaf:
                self.filter_tree(child)
                if child.is_leaf:
                    if not self.is_included(child) or self.is_excluded(child):
                        child.parent = None
            else:
                if not self.is_included(child) or self.is_excluded(child):
                    child.parent = None

    def root_path(self, node: Node, default_branch: Optional[str]) -> str:
        """Get the root path for a node.
        
        Args:
            node: Node to get path for
            
        Returns:
            Path string
        """
        parts = [str(n.name) for n in node.path]
        if default_branch:
            parts.append(default_branch)
        path = "/".join(parts)
        return path

    def make_node(self, type: str, name: str, parent: Node, url: str,
                  *, default_branch: Optional[str] = None,
                  protected_branches: Optional[List[str]] = None) -> Node:
        """Create a new node in the tree.
        
        Args:
            type: Node type
            name: Node name
            parent: Parent node
            url: Node URL
            
        Returns:
            Created node
        """
        node = Node(name=name, parent=parent, url=url, type=type)
        node.root_path = self.root_path(node, default_branch)
        if default_branch:
            node.default_branch = default_branch
        if protected_branches:
            node.protected_branches = protected_branches
        return node

    def add_projects(self, parent: Node, projects: List[Project]) -> None:
        """Add projects to the tree.
        
        Args:
            parent: Parent node
            projects: List of projects to add
            
        Raises:
            GitlabTreeError: If project addition fails
        """
        for project in projects:
            try:
                project_id = project.name if self.naming == FolderNaming.NAME else project.path
                project_url = project.ssh_url_to_repo if self.method is CloneMethod.SSH else project.http_url_to_repo
                if self.token is not None and self.method is CloneMethod.HTTP:
                    if not self.hide_token:
                        project_url = project_url.replace('://', f'://gitlab-token:{self.token}@')
                        log.debug("Generated URL: %s", project_url)
                    else:
                        log.debug("Hiding token from project url: %s", project_url)

                default_branch = None
                if self.naming == FolderNaming.BRANCH:
                    default_branch = getattr(project, "default_branch", None)

                protected_branches = None
                if self.protected_branches:
                    project = self.gitlab.projects.get(project.id)
                    try:
                        protected_branches = [branch.name for branch in project.protectedbranches.list()]
                    except GitlabHttpError as error:
                        if error.response_code == 403:
                            log.error("403 error while getting protected branches for project %s: %s", project.name, error.error_message)
                        else:
                            raise

                node = self.make_node("project", project_id, parent, url=project_url,
                                      default_branch=default_branch, protected_branches=protected_branches)
                self.progress.show_progress(node.name, 'project')
            except Exception as e:
                log.error("Failed to add project %s: %s", project.name, str(e))
                continue

    def get_projects(self, group: Group, parent: Node) -> None:
        """Get projects for a group.
        
        Args:
            group: Group to get projects for
            parent: Parent node
        """
        try:
            projects = group.projects.list(archived=self.archived, with_shared=self.include_shared, get_all=True)
            self.progress.update_progress_length(len(projects))
            self.add_projects(parent, projects)
            
            if self.include_shared and hasattr(group, 'shared_projects'):
                shared_projects = group.shared_projects.list(get_all=True)
                self.progress.update_progress_length(len(shared_projects))
                self.add_projects(parent, shared_projects)
        except GitlabListError as error:
            log.error("Error getting projects on %s id: [%s] error message: [%s]", 
                     group.name, group.id, error.error_message)
            # Continue execution instead of raising an exception

    def get_subgroups(self, group: Group, parent: Node) -> None:
        """Get subgroups for a group.
        
        Args:
            group: Group to get subgroups for
            parent: Parent node
        """
        try:
            subgroups = group.subgroups.list(as_list=False, get_all=True)
            self.progress.update_progress_length(len(subgroups))
            for subgroup_def in subgroups:
                try:
                    subgroup = self.gitlab.groups.get(subgroup_def.id)
                    subgroup_id = subgroup.name if self.naming == FolderNaming.NAME else subgroup.path
                    node = self.make_node("subgroup", subgroup_id, parent, url=subgroup.web_url)
                    self.progress.show_progress(node.name, 'group')
                    self.get_subgroups(subgroup, node)
                    self.get_projects(subgroup, node)
                except GitlabGetError as error:
                    if error.response_code == 404:
                        log.error(f"{error.response_code} error while getting subgroup with name: {group.name} [id: {group.id}]. Check your permissions as you may not have access to it. Message: {error.error_message}")
                        continue
                    log.error(f"Error getting subgroup: {error.error_message}")
                    continue
        except GitlabListError as error:
            if error.response_code == 404:
                log.error(f"{error.response_code} error while listing subgroup with name: {group.name} [id: {group.id}]. Check your permissions as you may not have access to it. Message: {error.error_message}")
            else:
                log.error(f"Failed to get subgroups for group {group.name}: {error.error_message}")
            # Continue execution instead of raising an exception

    def load_gitlab_tree(self) -> None:
        """Load the GitLab tree structure."""
        log.debug("Starting group search with archived: %s search term: %s", self.archived, self.group_search)
                    
        try:
            groups = self.gitlab.groups.list(as_list=False, archived=self.archived, get_all=True, search=self.group_search)
            self.progress.init_progress(len(groups))
            for group in groups:
                try:
                    if group.parent_id is None:
                        group_id = group.name if self.naming == FolderNaming.NAME else group.path
                        node = self.make_node("group", group_id, self.root, url=group.web_url)
                        self.progress.show_progress(node.name, 'group')
                        self.get_subgroups(group, node)
                        self.get_projects(group, node)
                except Exception as e:
                    log.error(f"Error processing group {group.name}: {str(e)}")
                    continue

            elapsed = self.progress.finish_progress()
            log.debug("Loading projects tree from gitlab took [%s]", elapsed)
        except Exception as e:
            log.error(f"Failed to load GitLab tree: {str(e)}")
            # Continue execution instead of raising an exception

    def load_file_tree(self) -> None:
        """Load tree structure from a YAML file."""
        try:
            with open(self.in_file, 'r') as stream:
                dct = yaml.safe_load(stream)
                self.root = DictImporter().import_(dct)
        except Exception as e:
            log.error(f"Failed to load tree from file {self.in_file}: {str(e)}")
            # Continue execution instead of raising an exception

    def load_user_tree(self) -> None:
        """Load user's personal projects."""
        log.debug("Starting user project search with archived: %s", self.archived)
        try:
            user = self.gitlab.users.get(self.gitlab.user.id)
            username = user.username
            projects = user.projects.list(as_list=False, archived=self.archived, get_all=True)
            self.progress.init_progress(len(projects))
            root = self.make_node("group", f"{username}-personal-projects", self.root, url=f"{self.url}/users/{username}/projects")
            self.add_projects(root, projects)
        except Exception as e:
            log.error(f"Failed to load user projects: {str(e)}")
            # Continue execution instead of raising an exception

    def load_tree(self) -> None:
        """Load the tree structure from appropriate source."""
        try:
            if self.in_file:
                log.debug("Loading tree from file [%s]", self.in_file)
                self.load_file_tree()
            elif self.user_projects:
                log.debug("Loading user personal projects from gitlab server [%s]", self.url)
                self.load_user_tree()
            else:
                log.debug("Loading projects tree from gitlab server [%s]", self.url)
                self.load_gitlab_tree()

            log.debug("Fetched root node with [%d] projects", len(self.root.leaves))
            self.filter_tree(self.root)
        except Exception as e:
            log.error(f"Failed to load tree: {str(e)}")
            # Continue execution instead of raising an exception

    def print_tree(self, format: PrintFormat = PrintFormat.TREE) -> None:
        """Print the tree in specified format.
        
        Args:
            format: Print format to use
            
        Raises:
            GitlabTreeError: If printing fails
        """
        try:
            if format is PrintFormat.TREE:
                self.print_tree_native()
            elif format is PrintFormat.YAML:
                self.print_tree_yaml()
            elif format is PrintFormat.JSON:
                self.print_tree_json()
            else:
                raise GitlabTreeError(f"Invalid print format: {format}")
        except Exception as e:
            raise GitlabTreeError(f"Failed to print tree: {str(e)}")

    def print_tree_native(self) -> None:
        """Print tree in native format."""
        for pre, _, node in RenderTree(self.root):
            line = ""
            if node.is_root:
                line = f"{pre}root [{self.url}]"
            else:
                line = f"{pre}{node.name} [{node.root_path}]"
            print(line)

    def print_tree_yaml(self) -> None:
        """Print tree in YAML format."""
        dct = DictExporter().export(self.root)
        print(yaml.dump(dct, default_flow_style=False))

    def print_tree_json(self) -> None:
        """Print tree in JSON format."""
        exporter = JsonExporter(indent=2, sort_keys=True)
        print(exporter.export(self.root))

    def sync_tree(self, dest: str) -> None:
        """Sync the tree to destination.
        
        Args:
            dest: Destination path
            
        Raises:
            GitlabTreeError: If sync fails
        """
        try:
            log.debug("Going to clone/pull [%s] groups and [%s] projects",
                     len(self.root.descendants) - len(self.root.leaves), len(self.root.leaves))
            sync_tree(self.root, dest, concurrency=self.concurrency,
                     disable_progress=self.disable_progress, recursive=self.recursive,
                     use_fetch=self.use_fetch, hide_token=self.hide_token)
        except Exception as e:
            raise GitlabTreeError(f"Failed to sync tree: {str(e)}")

    def is_empty(self) -> bool:
        """Check if the tree is empty.
        
        Returns:
            True if tree is empty, False otherwise
        """
        return self.root.height < 1
