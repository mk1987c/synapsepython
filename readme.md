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
    
3. upload wheel package to storage account.
    a.  Create a powershell script.
    


4. from storage account import wheel package to synapse workspace.
5. and from synapse workspace import library to required cluster.


