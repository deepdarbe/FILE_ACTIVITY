@{
    RootModule = 'FileActivity.psm1'
    ModuleVersion = '1.0.0'
    GUID = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
    Author = 'deepdarbe'
    Description = 'PowerShell wrapper for FILE ACTIVITY REST API.'
    PowerShellVersion = '5.1'
    FunctionsToExport = @(
        'Get-FileActivityScan',
        'Start-FileActivityScan',
        'Get-FileActivitySummary',
        'Get-FileActivityDuplicates',
        'Get-FileActivityRansomware',
        'Invoke-FileActivityArchive',
        'Get-FileActivityAudit',
        'Test-FileActivityAuditChain',
        'Set-FileActivityBaseUrl',
        'Get-FileActivityBaseUrl'
    )
    CmdletsToExport = @()
    VariablesToExport = @()
    AliasesToExport = @()
}
