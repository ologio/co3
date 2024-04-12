from typing import TypeVar

import sqlalchemy as sa


SQLTableLike = TypeVar('SQLTableLike', bound=sa.Table | sa.Subquery | sa.Join)
