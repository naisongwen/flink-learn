#using batch table environment to execute the queries
from pyflink.table import EnvironmentSettings, BatchTableEnvironment

env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = BatchTableEnvironment.create(environment_settings=env_settings)

orders = table_env.from_elements([('Jack', 'FRANCE', 10), ('Rose', 'ENGLAND', 30), ('Jack', 'FRANCE', 20)],
                                 ['name', 'country', 'revenue'])

# compute revenue for all customers from France
revenue = orders \
    .select(orders.name, orders.country, orders.revenue) \
    .where(orders.country == 'FRANCE') \
    .group_by(orders.name) \
    .select(orders.name, orders.revenue.sum.alias('rev_sum'))
    
revenue.to_pandas()
