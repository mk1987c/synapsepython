# Install the engineering wheel onto a spark pool
# Todo, needs to be parameterized.

$workspace = "syanpsepython"
$pool = "cluster"
$resourceGroup = "rg_yt_resource_gp_mk"

# Create and install engineering python library      #
##############################################

$wheel_storage_account_name = "sasyanpse" #todo make environment dependent
$wheel_container_name = "library"

#$script_root = $PSScriptRoot

#& $PSScriptRoot/create_upload_wheel.ps1 -storage_account_name $wheel_storage_account_name -container_name $wheel_container_name


# Get project root path
#$project_root = (Split-Path $PSScriptRoot -Parent)

# Get the wheel name
$wheel_name = "dataengineering001-1.0-py3-none-any.whl"


#Add wheel to pool
Write-Host "Adding wheel '$wheel_name', to pool '$pool'.\n"
az synapse spark pool update --name $pool --workspace-name $workspace `
--resource-group $resourceGroup `
--package-action Add `
--package $wheel_name