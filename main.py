import sys
import asyncio
import threading
import logging
import time

from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                              QTextEdit, QSystemTrayIcon, QMenu)
from PyQt6.QtGui import QPainter, QPen, QColor, QConicalGradient, QIcon, QAction
from PyQt6.QtCore import Qt, QTimer, QPointF, pyqtSignal, QObject

from cruzebot import (
    run_telegram_bot, run_stats_pusher, 
    load_total_signals, load_active_targets, load_managed_trades,
    bot_log, push_log
)

# ── LOG REDIRECTION ──────────────────────────────────────────────────────────
class QtLogHandler(logging.Handler, QObject):
    """Custom logging handler that emits a signal for each log record."""
    new_log = pyqtSignal(str)

    def __init__(self):
        logging.Handler.__init__(self)
        QObject.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        self.new_log.emit(msg)

# ── RADAR WIDGET ─────────────────────────────────────────────────────────────
class RadarWidget(QWidget):
    """A custom widget that draws a spinning radar effect."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(120, 120)
        self.angle = 0
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.rotate)
        self.timer.start(30) # ~33 FPS

    def rotate(self):
        self.angle = (self.angle + 3) % 360
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        rect = self.rect()
        center = rect.center()
        radius = min(rect.width(), rect.height()) // 2 - 10

        # Background circle
        painter.setPen(QPen(QColor(0, 255, 0, 80), 1))
        painter.drawEllipse(center, radius, radius)
        painter.drawEllipse(center, radius // 2, radius // 2)

        # Spinning beam
        gradient = QConicalGradient(QPointF(center), -self.angle)
        gradient.setColorAt(0, QColor(0, 255, 0, 180))
        gradient.setColorAt(0.1, QColor(0, 255, 0, 50))
        gradient.setColorAt(0.5, QColor(0, 255, 0, 0))
        
        painter.setBrush(gradient)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.drawPie(center.x() - radius, center.y() - radius, 
                        radius * 2, radius * 2, self.angle * 16, 60 * 16)

# ── MAIN WINDOW ──────────────────────────────────────────────────────────────
class CruzeBotGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CruzeBot — Live Terminal")
        self.setMinimumSize(500, 650)
        self.setStyleSheet("background-color: #121212; color: #00FF00; font-family: 'Consolas';")

        # 1. Setup UI
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # Radar at the top
        self.radar = RadarWidget()
        layout.addWidget(self.radar, alignment=Qt.AlignmentFlag.AlignCenter)

        # Log Window
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setStyleSheet("""
            background-color: #0c0c0c; 
            border: 1px solid #333; 
            font-size: 12px;
            padding: 5px;
        """)
        layout.addWidget(self.log_view)

        # 2. Setup Logging Redirection
        self.log_handler = QtLogHandler()
        self.log_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname).4s] %(message)s', '%H:%M:%S'))
        self.log_handler.new_log.connect(self.append_log)
        
        # Attach to all relevant loggers
        bot_log.addHandler(self.log_handler)
        push_log.addHandler(self.log_handler)
        logging.getLogger().addHandler(self.log_handler) # Catch everything

        # 3. Tray Icon logic
        self.create_tray_icon()

        # 4. Start Bot Threads
        self.start_bot()

    def create_tray_icon(self):
        self.tray_icon = QSystemTrayIcon(self)
        # Use a dot as fallback if icon not found
        self.tray_icon.setIcon(QIcon.fromTheme("system-run")) 
        
        tray_menu = QMenu()
        show_action = QAction("Show Terminal", self)
        show_action.triggered.connect(self.showNormal)
        
        exit_action = QAction("Exit Bot", self)
        exit_action.triggered.connect(self.real_exit)
        
        tray_menu.addAction(show_action)
        tray_menu.addSeparator()
        tray_menu.addAction(exit_action)
        
        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.show()
        self.tray_icon.activated.connect(self.on_tray_click)

    def on_tray_click(self, reason):
        if reason == QSystemTrayIcon.ActivationReason.Trigger:
            if self.isVisible():
                self.hide()
            else:
                self.showNormal()

    def closeEvent(self, event):
        """Override close to minimize to tray instead."""
        if self.tray_icon.isVisible():
            self.hide()
            self.tray_icon.showMessage(
                "CruzeBot",
                "Bot is still running in the tray.",
                QSystemTrayIcon.MessageIcon.Information,
                2000
            )
            event.ignore()
        else:
            self.real_exit()

    def real_exit(self):
        self.tray_icon.hide()
        QApplication.quit()
        sys.exit(0)

    def append_log(self, text):
        self.log_view.append(text)
        cursor = self.log_view.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self.log_view.setTextCursor(cursor)

    def start_bot(self):
        # 1. Load persistent state
        load_total_signals()
        load_active_targets()
        load_managed_trades()

        # 2. Stats Pusher in background
        threading.Thread(target=run_stats_pusher, name="StatsPusher", daemon=True).start()

        # 3. Telegram Bot in background
        # Note: Telethon needs its own event loop if not on main thread
        threading.Thread(target=self.run_async_bot, name="TelegramBot", daemon=True).start()

    def run_async_bot(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_telegram_bot())
        except Exception as e:
            bot_log.error("Telegram Bot loop error: %s", e)
        finally:
            loop.close()

def main():
    app = QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)

    terminal = CruzeBotGUI()
    terminal.show()

    sys.exit(app.exec())

if __name__ == "__main__":
    main()
