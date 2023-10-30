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
    [string]$container_name = "",

    [Parameter()]
    [string]$path = ""
)

# Input parsing
if ($path -ne "") {
    $path = $path.TrimEnd('\/')
    $path = "$path/"
}

# Get project root path
$project_root = (Split-Path $PSScriptRoot -Parent)

# Create the wheel
python -m pip install --upgrade pip
python -m pip install wheel
python  C:/Users/makum/Documents/Deepak/synapsepython/setup.py bdist_wheel -d $project_root/wheels


$wheel_sourcepath = "C:/Users/makum/Documents/Deepak/synapsepython/dist/"
$wheel_filename = "dataengineering-1.0-py3-none-any"

$storage_fullpath = "abfss://$container_name@$storage_account_name.dfs.core.windows.net/dataengineering/"
Write-Host "Uploading wheel '$wheel_filename', from location '$wheel_sourcepath' to remote storage location '$storage_fullpath'."

# Upload the wheel to Azure storage account

