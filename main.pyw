import sys
import os
import asyncio
import threading
import logging
import traceback
import time

# ── SILENT CRASH GUARD ───────────────────────────────────────────────────────
# .pyw files run with pythonw.exe (no console), so all errors are invisible.
# This redirects crashes to a log file next to the script.
_log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cruzebot_error.log")

def _excepthook(exc_type, exc_value, exc_tb):
    with open(_log_path, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"CRASH at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        traceback.print_exception(exc_type, exc_value, exc_tb, file=f)
    sys.__excepthook__(exc_type, exc_value, exc_tb)

sys.excepthook = _excepthook

try:
    from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                                  QTextEdit, QSystemTrayIcon, QMenu)
    from PyQt6.QtGui import QPainter, QPen, QColor, QConicalGradient, QIcon, QAction, QPixmap
    from PyQt6.QtCore import Qt, QTimer, QPointF, pyqtSignal, QObject
except Exception:
    with open(_log_path, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"PyQt6 IMPORT FAILED at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        traceback.print_exc(file=f)
    sys.exit(1)

try:
    from cruzebot import (
        run_telegram_bot, run_stats_pusher,
        load_total_signals, load_active_targets, load_managed_trades,
        bot_log, push_log
    )
except Exception:
    with open(_log_path, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"cruzebot IMPORT FAILED at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        traceback.print_exc(file=f)
    sys.exit(1)

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
        cx = float(center.x())
        cy = float(center.y())
        radius = min(rect.width(), rect.height()) // 2 - 10

        # Background circle
        painter.setPen(QPen(QColor(0, 255, 0, 80), 1))
        painter.drawEllipse(center, radius, radius)
        painter.drawEllipse(center, radius // 2, radius // 2)

        # Spinning beam
        gradient = QConicalGradient(QPointF(cx, cy), -self.angle)
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
        self._force_quit = False

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

        # Attach only to root logger — bot_log and push_log propagate up
        # automatically, so adding to all three caused every message to appear twice.
        logging.getLogger().addHandler(self.log_handler)

        # 3. Tray Icon logic
        self.app_icon = self.create_bot_icon()
        self.setWindowIcon(self.app_icon)
        self.create_tray_icon()

        # 4. Start Bot Threads
        self.start_bot()

    def create_bot_icon(self):
        """Creates a vibrant green icon programmatically."""
        pixmap = QPixmap(100, 100)
        pixmap.fill(Qt.GlobalColor.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setBrush(QColor(0, 255, 0))
        painter.setPen(QPen(QColor(255, 255, 255), 4))
        painter.drawEllipse(10, 10, 80, 80)
        painter.end()
        return QIcon(pixmap)

    def create_tray_icon(self):
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(self.app_icon)
        self.tray_icon.setToolTip("CruzeBot (Active)")

        tray_menu = QMenu()
        show_action = QAction("Show Terminal", self)
        show_action.triggered.connect(self.showNormal)

        exit_action = QAction("Exit Bot", self)
        exit_action.triggered.connect(self.real_exit)

        tray_menu.addAction(show_action)
        tray_menu.addSeparator()
        tray_menu.addAction(exit_action)

        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.setVisible(True)
        self.tray_icon.show()
        self.tray_icon.activated.connect(self.on_tray_click)

    def on_tray_click(self, reason):
        if reason == QSystemTrayIcon.ActivationReason.Trigger:
            if self.isVisible():
                self.hide()
            else:
                self.showNormal()
                self.activateWindow()

    def changeEvent(self, event):
        """Minimizing keeps window in taskbar — does NOT auto-hide to tray."""
        # We intentionally do NOT call self.hide() here anymore,
        # so the window stays visible in the Windows taskbar when minimized.
        super().changeEvent(event)

    def closeEvent(self, event):
        """Override close to minimize to tray instead of quitting."""
        if not self._force_quit and self.tray_icon.isVisible():
            self.hide()
            self.tray_icon.showMessage(
                "CruzeBot",
                "Bot is still running in the tray. Right-click the tray icon to exit.",
                QSystemTrayIcon.MessageIcon.Information,
                2000
            )
            event.ignore()
        else:
            self.real_exit()

    def real_exit(self):
        self._force_quit = True
        self.tray_icon.hide()
        QApplication.quit()
        sys.exit(0)

    def append_log(self, text):
        self.log_view.append(text)
        cursor = self.log_view.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self.log_view.setTextCursor(cursor)

    def start_bot(self):
        load_total_signals()
        load_active_targets()
        load_managed_trades()
        threading.Thread(target=run_stats_pusher, name="StatsPusher", daemon=True).start()
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
    try:
        app = QApplication(sys.argv)
        app.setQuitOnLastWindowClosed(False)

        terminal = CruzeBotGUI()
        terminal.show()
        terminal.raise_()
        terminal.activateWindow()

        sys.exit(app.exec())
    except Exception:
        with open(_log_path, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"RUNTIME ERROR at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            traceback.print_exc(file=f)
        raise

if __name__ == "__main__":
    main()
