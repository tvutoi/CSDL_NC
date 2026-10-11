# =====================================================================================
# SCRIPT TỰ ĐỘNG CẤU HÌNH MẠNG LAN WIFI PHÒNG LAB & MỞ CỔNG FIREWALL
# Chạy bằng quyền Administrator trên cả 4 máy thành viên khi vào phòng LAB
# =====================================================================================

Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "  CẤU HÌNH MẠNG LAN WIFI PHÒNG LAB - ĐỀ TÀI 2 (MYSQL GR + MONGODB)" -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan

# 1. Hiển thị địa chỉ IPv4 hiện tại của card WiFi / Ethernet đang kết nối
Write-Host "`n[1] Thông tin Địa chỉ IPv4 và MAC Address của máy hiện tại:" -ForegroundColor Yellow
Get-NetIPConfiguration | Where-Object { $_.IPv4DefaultGateway -ne $null } | ForEach-Object {
    $adapter = Get-NetAdapter -InterfaceIndex $_.InterfaceIndex
    Write-Host "  + Card mạng : $($adapter.Name) ($($adapter.InterfaceDescription))"
    Write-Host "  + MAC Addr  : $($adapter.MacAddress)"
    Write-Host "  + IPv4 Addr : $($_.IPv4Address.IPAddress)" -ForegroundColor Green
    Write-Host "  + Gateway   : $($_.IPv4DefaultGateway.NextHop)"
}

# 2. Đặt Profile mạng sang Private (Để Windows không chặn kết nối ngang hàng giữa các máy trong WiFi LAB)
Write-Host "`n[2] Chuyển Network Profile sang Private để cho phép kết nối LAN nội bộ..." -ForegroundColor Yellow
Get-NetConnectionProfile | Set-NetConnectionProfile -NetworkCategory Private -ErrorAction SilentlyContinue
Write-Host "  -> Đã thiết lập Network Category = Private!" -ForegroundColor Green

# 3. Mở cổng Firewall cho MySQL (3306), Group Replication (33061), MongoDB (27017), Web App (8000) và ICMP Ping
Write-Host "`n[3] Mở các cổng Firewall cần thiết (3306, 33061, 27017, 8000 & Ping ICMP)..." -ForegroundColor Yellow
Remove-NetFirewallRule -DisplayName "CTH574T_Nhom2_DB_Ports" -ErrorAction SilentlyContinue
New-NetFirewallRule -DisplayName "CTH574T_Nhom2_DB_Ports" -Direction Inbound -LocalPort 3306,33061,27017,8000 -Protocol TCP -Action Allow -Profile Any | Out-Null
New-NetFirewallRule -DisplayName "CTH574T_Nhom2_ICMP_Ping" -Direction Inbound -Protocol ICMPv4 -IcmpType 8 -Action Allow -Profile Any | Out-Null
Write-Host "  -> Đã mở thông cổng TCP 3306, 33061, 27017, 8000 và ICMP Ping!" -ForegroundColor Green

Write-Host "`n=====================================================================" -ForegroundColor Cyan
Write-Host "  HOÀN TẤT! HÃY GHI NHỚ ĐỊA CHỈ IPv4 Ở TRÊN ĐỂ NHẬP VÀO CẤU HÌNH CỤM" -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan

