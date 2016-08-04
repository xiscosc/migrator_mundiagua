from clients import migrate_clients
from interventions import migrate_interventions
from budgets import migrate_budgets, migrate_budgets_repair
from repairs import migrate_repairs
from messages import migrate_messages

# LOAD FIXTURES FIRST!

migrate_clients()
migrate_interventions()
migrate_budgets()
migrate_repairs()
migrate_messages()
migrate_budgets_repair()
print("ALL CORRECT!")