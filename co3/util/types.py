from typing import TypeVar

import sqlalchemy as sa


TableLike = TypeVar('TableLike', bound=sa.Table | sa.Subquery | sa.Join)
