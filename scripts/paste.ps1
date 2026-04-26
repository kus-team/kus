# Сохраняет картинку из буфера обмена в D:\KUS\screen.png.
# Использование:  ! D:\KUS\paste.bat   (через ! в чате Claude Code)
# или просто двойной клик по D:\KUS\paste.bat
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing

$out = "D:\KUS\screen.png"

if ([System.Windows.Forms.Clipboard]::ContainsImage()) {
    $img = [System.Windows.Forms.Clipboard]::GetImage()
    $img.Save($out, [System.Drawing.Imaging.ImageFormat]::Png)
    $img.Dispose()
    $size = (Get-Item $out).Length
    Write-Host "OK: saved $out  ($([math]::Round($size/1024)) KB, $($img.Width)x$($img.Height))"
}
elseif ([System.Windows.Forms.Clipboard]::ContainsFileDropList()) {
    # На случай если ты скопировал FILE из проводника
    $first = [System.Windows.Forms.Clipboard]::GetFileDropList() | Select-Object -First 1
    Copy-Item -Path $first -Destination $out -Force
    Write-Host "OK: copied $first to $out"
}
else {
    Write-Host "NOTHING: в буфере нет картинки. Сделай Win+Shift+S → выдели → Ctrl+C → запусти снова."
    exit 1
}
