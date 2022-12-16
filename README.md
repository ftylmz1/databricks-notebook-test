# databricks-notebook-test


## Create databricks scope: 'my_scope'


## Create a secret to store the password of your Service Principal with name: 'my_sp'


## Add secrects to databricks key-vault
databricks secrets put --scope my_scope --key storage_account
databricks secrets put --scope my_scope --key application_id
databricks secrets put --scope my_scope --key directory_id
