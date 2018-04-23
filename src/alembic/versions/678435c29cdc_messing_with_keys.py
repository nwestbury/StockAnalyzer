"""messing with keys

Revision ID: 678435c29cdc
Revises: 
Create Date: 2017-08-24 18:49:13.535970

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '678435c29cdc'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('migrate_version')
    op.drop_constraint('fiscal_year_unique', 'ms_financials', type_='unique')
    op.add_column('price_history', sa.Column('action', sa.String(), autoincrement=False, nullable=True))
    op.add_column('price_history', sa.Column('value', sa.Float(), autoincrement=False, nullable=True))
    op.drop_column('price_history', 'adj close')
    op.drop_column('price_history', 'close')
    op.drop_column('price_history', 'high')
    op.drop_column('price_history', 'low')
    op.drop_column('price_history', 'open')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('price_history', sa.Column('open', sa.REAL(), autoincrement=False, nullable=True))
    op.add_column('price_history', sa.Column('low', sa.REAL(), autoincrement=False, nullable=True))
    op.add_column('price_history', sa.Column('high', sa.REAL(), autoincrement=False, nullable=True))
    op.add_column('price_history', sa.Column('close', sa.REAL(), autoincrement=False, nullable=True))
    op.add_column('price_history', sa.Column('adj close', sa.REAL(), autoincrement=False, nullable=True))
    op.drop_column('price_history', 'value')
    op.drop_column('price_history', 'action')
    op.create_unique_constraint('fiscal_year_unique', 'ms_financials', ['ticker', 'exchange', 'fiscal_year', 'period'])
    op.create_table('migrate_version',
    sa.Column('repository_id', sa.VARCHAR(length=250), autoincrement=False, nullable=False),
    sa.Column('repository_path', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('version', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('repository_id', name='migrate_version_pkey')
    )
    # ### end Alembic commands ###