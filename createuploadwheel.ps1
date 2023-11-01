##########################################################################
## create_upload_wheel.ps1                                              ##
## Used for creating a wheel for the 'eva' library, and uploading that  ##
## to a specified folder on an Azure Storage Account                    ##
##########################################################################

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$storage_account_name = "",

    [Parameter(Mandatory)]
    [string]$container_name = ""
)

$wheel_sourcepath = "C:/Users/makum/Documents/Deepak/synapsepython/dist/"
$wheel_filename = "dataengineering001-1.0-py3-none-any.whl"

$storage_fullpath = "abfss://$container_name@$storage_account_name.dfs.core.windows.net/"
Write-Host "Uploading wheel '$wheel_filename', from location '$wheel_sourcepath' to remote storage location '$storage_fullpath'."

# Upload the wheel to Azure storage account
$WHEEL_UPLOAD_STATUS=az storage blob upload `
    --account-name $storage_account_name `
    --container-name $container_name `
    --name "$wheel_filename" `
    --file $wheel_sourcepath$wheel_filename `
    --overwrite `
    --auth-mode login `
    | ConvertFrom-Json

