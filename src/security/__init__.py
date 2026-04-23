"""Security subsystem.

Modules:
    ransomware_detector  -- detect ransomware patterns (rename velocity,
                            risky extensions, mass deletion, canary access)
    smb_session          -- thin wrapper around PowerShell Get-/Close-SmbSession
                            for auto-killing suspicious SMB sessions
"""
