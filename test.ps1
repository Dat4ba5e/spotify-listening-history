# Da "D:" ein DVD-Rom Laufwerk ist kann es nicht durch das normale CMDLET zum ändern eines Laufwerksbuchstaben (Set-Partition –NewDriveLetter) geändert werden. Daher hier eine Alternative, um die Umbenennung durchzusetzen.
Get-WmiObject -Class Win32_volume -Filter "DriveLetter = 'd:'" | fo-reach-object{ $disk = $_ }
$disk.DriveLetter = "Z:"
Set-WmiInstance -InputObject $disk