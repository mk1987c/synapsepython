Initialized by Azure Synapse Workspace!

# virtual_envrironment_name - synapsepython-bEsg9Nbd

Steps 
1. Create a setup.py without below content

            from setuptools import setup

            setup(
                name='testWheel',
                version='1.0',
                packages=['.testWheel'],
            )

2. Create a wheel file using below command.

    python setup.py bdist_wheel

3. Login to Azure account using az account

    az login
    
4. upload wheel package to storage account.
    a. Create a powershell script.
        - $project_root = $PWD  # you can set the $project_root variable using the $PWD automatic variable, which represents the current working directory:
        - echo $project_root   # verify that the variable is set correctly by echoing its value:

    b. invoke powersheel script with below command
        - ./createuploadwheel.ps1 -storage_account_name "sasyanpse" -container_name "library"
C:\Users\makum\Downloads\streaming_Training
5. from storage account import wheel package to synapse workspace.

6. once imported in synpase workspace we can import in cluster using 
    - ./install_package.ps1


6. and from synapse workspace import library to required cluster.


