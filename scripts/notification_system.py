#!/usr/bin/env python3
"""
Sistema di notifiche per la dashboard MIDA
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import os

class NotificationSystem:
    def __init__(self):
        self.notifications_file = "data/notifications.json"
        self.load_notifications()
    
    def load_notifications(self):
        """Carica le notifiche dal file JSON"""
        if os.path.exists(self.notifications_file):
            try:
                with open(self.notifications_file, 'r', encoding='utf-8') as f:
                    self.notifications = json.load(f)
            except:
                self.notifications = []
        else:
            self.notifications = []
    
    def save_notifications(self):
        """Salva le notifiche nel file JSON"""
        os.makedirs(os.path.dirname(self.notifications_file), exist_ok=True)
        with open(self.notifications_file, 'w', encoding='utf-8') as f:
            json.dump(self.notifications, f, ensure_ascii=False, indent=2)
    
    def add_notification(self, title, message, notification_type="info"):
        """Aggiunge una nuova notifica"""
        notification = {
            "id": len(self.notifications) + 1,
            "title": title,
            "message": message,
            "type": notification_type,
            "timestamp": datetime.now().isoformat(),
            "read": False
        }
        self.notifications.append(notification)
        self.save_notifications()
    
    def mark_as_read(self, notification_id):
        """Segna una notifica come letta"""
        for notif in self.notifications:
            if notif["id"] == notification_id:
                notif["read"] = True
        self.save_notifications()
    
    def get_unread_count(self):
        """Restituisce il numero di notifiche non lette"""
        return len([n for n in self.notifications if not n["read"]])
    
    def get_recent_notifications(self, limit=5):
        """Restituisce le notifiche più recenti"""
        return sorted(self.notifications, key=lambda x: x["timestamp"], reverse=True)[:limit]

def check_data_anomalies(df):
    """Controlla anomalie nei dati e genera notifiche"""
    notifications = []
    
    # Controlla missioni con date mancanti
    missing_dates = df[df['data_inizio'].isna() | df['data_fine'].isna()]
    if len(missing_dates) > 0:
        notifications.append({
            "title": "⚠️ Date Mancanti",
            "message": f"Trovate {len(missing_dates)} missioni con date mancanti",
            "type": "warning"
        })
    
    # Controlla missioni con costi anomali
    avg_cost = df['costo_totale'].mean()
    std_cost = df['costo_totale'].std()
    anomaly_threshold = avg_cost + 2 * std_cost
    cost_anomalies = df[df['costo_totale'] > anomaly_threshold]
    if len(cost_anomalies) > 0:
        notifications.append({
            "title": "💰 Costi Anomali",
            "message": f"Trovate {len(cost_anomalies)} missioni con costi anomali",
            "type": "warning"
        })
    
    # Controlla missioni attive
    current_date = pd.Timestamp.now()
    active_missions = df[df['data_fine'] > current_date]
    if len(active_missions) > 0:
        notifications.append({
            "title": "🟢 Missioni Attive",
            "message": f"Attualmente ci sono {len(active_missions)} missioni attive",
            "type": "success"
        })
    
    return notifications

def display_notifications():
    """Mostra le notifiche nella sidebar"""
    st.sidebar.markdown("---")
    st.sidebar.header("🔔 Notifiche")
    
    # Inizializza il sistema di notifiche
    notification_system = NotificationSystem()
    
    # Controlla anomalie nei dati
    df = st.session_state.get('df', None)
    if df is not None:
        anomalies = check_data_anomalies(df)
        for anomaly in anomalies:
            notification_system.add_notification(
                anomaly["title"], 
                anomaly["message"], 
                anomaly["type"]
            )
    
    # Mostra notifiche recenti
    recent_notifications = notification_system.get_recent_notifications()
    unread_count = notification_system.get_unread_count()
    
    if unread_count > 0:
        st.sidebar.markdown(f"**📬 {unread_count} nuove notifiche**")
    
    for notif in recent_notifications:
        # Icona per tipo di notifica
        icons = {
            "info": "ℹ️",
            "warning": "⚠️", 
            "success": "✅",
            "error": "❌"
        }
        icon = icons.get(notif["type"], "ℹ️")
        
        # Colore per tipo di notifica
        colors = {
            "info": "#2196F3",
            "warning": "#FF9800",
            "success": "#4CAF50", 
            "error": "#F44336"
        }
        color = colors.get(notif["type"], "#2196F3")
        
        # Mostra notifica
        st.sidebar.markdown(f"""
        <div style="border-left: 4px solid {color}; padding-left: 10px; margin: 5px 0;">
            <strong>{icon} {notif['title']}</strong><br>
            <small>{notif['message']}</small><br>
            <small style="color: #666;">{datetime.fromisoformat(notif['timestamp']).strftime('%d/%m %H:%M')}</small>
        </div>
        """, unsafe_allow_html=True)
        
        # Pulsante per segnare come letta
        if not notif["read"]:
            if st.sidebar.button(f"✓ Segna come letta", key=f"read_{notif['id']}"):
                notification_system.mark_as_read(notif["id"])
                st.rerun()
    
    if not recent_notifications:
        st.sidebar.info("Nessuna notifica al momento")

def add_custom_notification(title, message, notification_type="info"):
    """Aggiunge una notifica personalizzata"""
    notification_system = NotificationSystem()
    notification_system.add_notification(title, message, notification_type)

if __name__ == "__main__":
    # Test del sistema di notifiche
    notification_system = NotificationSystem()
    notification_system.add_notification("Test", "Questa è una notifica di test", "info")
    print("Sistema di notifiche testato con successo!") 