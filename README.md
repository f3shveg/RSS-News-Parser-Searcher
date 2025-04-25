For rss service:

1.Create a systemd service file (for Linux systems):
sudo nano /etc/systemd/system/rss-monitor.service

[Unit]
Description=RSS Feed Monitor Service
After=network.target

[Service]
Type=simple
User=YOUR_USERNAME
WorkingDirectory=/path/to/your/project
ExecStart=/path/to/your/python /path/to/your/project/rss_daemon.py
Restart=always
RestartSec=60

[Install]
WantedBy=multi-user.target

2.Install required dependencies:
pip install python-daemon lockfile

# Start the service
sudo systemctl start rss-monitor

# Enable service to start on boot
sudo systemctl enable rss-monitor

# Check service status
sudo systemctl status rss-monitor

# View logs
tail -f feed_monitor.log

# Stop the service
sudo systemctl stop rss-monitor
