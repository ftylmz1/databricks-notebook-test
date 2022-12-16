# databricks-notebook-test


<a href="https://www.buymeacoffee.com/ftylmz1" target="_blank"><img src="https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png" alt="Buy Me A Coffee" style="height: 41px !important;width: 174px !important;box-shadow: 0px 3px 2px 0px rgba(190, 190, 190, 0.5) !important;-webkit-box-shadow: 0px 3px 2px 0px rgba(190, 190, 190, 0.5) !important;" ></a>





## Create databricks scope: 'my_scope'


## Create a secret to store the password of your Service Principal with name: 'my_sp'


## Add secrects to databricks key-vault
databricks secrets put --scope my_scope --key storage_account
databricks secrets put --scope my_scope --key application_id
databricks secrets put --scope my_scope --key directory_id
