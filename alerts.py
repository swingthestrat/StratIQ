import requests
import json

WEBHOOK_FILE = '/Users/nigeljohnson/AntiGravity/StratIQ/Discord/webhook'

def get_webhook_url():
    try:
        with open(WEBHOOK_FILE, 'r') as f:
            return f.read().strip()
    except Exception as e:
        print(f"Error reading webhook file: {e}")
        return None

def send_alert(title, desc, color=65280):
    url = get_webhook_url()
    if not url: return
    
    data = {
        "embeds": [{
            "title": title,
            "description": desc,
            "color": color
        }]
    }
    
    try:
        requests.post(url, json=data)
        print(f"Sent alert: {title}")
    except Exception as e:
        print(f"Error sending alert: {e}")

def process_alerts(ticker_alerts, theme_alerts):
    # Send Ticker Alerts
    for alert in ticker_alerts:
        # Map type to color
        color = 65280 # Green default
        if "Squeeze" in alert['type']: color = 16711935 # Fuchsia
        if "IML" in alert['type']: color = 3066993 # Teal
        if "Inside" in alert['type']: color = 16705372 # Orange
        
        title = f"{alert['ticker']} - {alert['type']}"
        desc = f"{alert['desc']}\\n**Price:** ${alert['price']:.2f}"
        send_alert(title, desc, color)
        
    # Send Theme Alerts
    for alert in theme_alerts:
        send_alert(alert['title'], alert['desc'], alert['color'])
