#from co3.resources.disk import DiskResource

from pathlib import Path

from co3.util import paths
from co3.resource import SelectableDomain


class DiskDomain(SelectableDomain):
    def select(
        self,
        path_list: str | Path | list[str | Path],
        glob:  str | None = None
    ) -> list[Path]:
        iter_path_kwargs = {'relative': True, 'no_dir': True}

        if type(path_list) is not list:
            path_list = [path_list]

        path_agg = set()
        for path in path_list:
            path_union = set()

            if glob is None:
                path_union = set(paths.iter_nested_paths(path, **iter_path_kwargs))
            else:
                path_union = set(paths.iter_glob_paths(glob, path, **iter_path_kwargs))

            path_agg = path_agg.union(( (path, head) for head in path_union ))

        return path_agg

