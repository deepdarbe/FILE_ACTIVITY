"""SMTP e-posta bildirim modulu.

Kullanici verimlilik skoru + uyumsuzluk raporunu hedef kullanicilara
HTML e-posta olarak gonderir, kopya (CC) olarak config'de tanimli
yonetici adresini ekler.

Konfigurasyon (config.yaml):

    smtp:
      enabled: false
      host: "smtp.firma.local"
      port: 587
      use_tls: true              # STARTTLS (587 uzerinde)
      use_ssl: false             # Implicit TLS (465 uzerinde) - use_tls ile birlikte false olmali
      username: ""
      password: ""               # Tavsiye: bos birakip SMTP_PASSWORD env var kullanin
      from_address: "fileactivity@firma.local"
      from_name: "File Activity System"
      timeout_seconds: 10

    notifications:
      admin_cc_email: ""         # Her bildirime CC eklenecek yonetici adresi
      subject_prefix: "[File Activity]"

Tum bildirimler gonderim logu (notification_log tablosu) tutulur:
  (id, username, email, subject, status, error, sent_at)

enabled=false ise modul no-op calisir, send_* None doner.
"""

import html
import json
import logging
import os
import smtplib
import ssl
from contextlib import contextmanager
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from typing import Optional

logger = logging.getLogger("file_activity.email_notifier")


class EmailNotifier:
    """SMTP uzerinden HTML bildirim e-postalari gonderir."""

    def __init__(self, db, config: dict):
        self.db = db
        smtp = config.get("smtp", {}) or {}
        notif = config.get("notifications", {}) or {}

        self.enabled = bool(smtp.get("enabled", False))
        self.host = smtp.get("host", "").strip()
        self.port = int(smtp.get("port", 587))
        self.use_tls = bool(smtp.get("use_tls", True))
        self.use_ssl = bool(smtp.get("use_ssl", False))
        self.username = smtp.get("username", "")
        # Sifre oncelik: env var > config
        self.password = os.environ.get("SMTP_PASSWORD") or smtp.get("password", "")
        self.from_address = smtp.get("from_address", "")
        self.from_name = smtp.get("from_name", "File Activity System")
        self.timeout = int(smtp.get("timeout_seconds", 10))

        self.admin_cc = (notif.get("admin_cc_email") or "").strip()
        self.subject_prefix = notif.get("subject_prefix", "[File Activity]")

        self._init_error: Optional[str] = None
        if not self.enabled:
            self._init_error = "config.smtp.enabled=false"
        elif not self.host or not self.from_address:
            self._init_error = "smtp.host veya smtp.from_address eksik"

        self._ensure_log_table()

        if self._init_error:
            logger.info("EmailNotifier devre disi: %s", self._init_error)

    @property
    def available(self) -> bool:
        return self._init_error is None

    def _ensure_log_table(self):
        with self.db.get_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS notification_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT,
                    email TEXT,
                    cc TEXT,
                    subject TEXT,
                    status TEXT NOT NULL,
                    error TEXT,
                    payload_json TEXT,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_notification_log_username_sent_at
                ON notification_log(username, sent_at DESC)
            """)

    # ──────────────────────────────────────────────
    # Alt seviye SMTP
    # ──────────────────────────────────────────────

    def _connect(self) -> smtplib.SMTP:
        """SMTP baglantisi ac. SSL/TLS ayarlarina gore uygun sinif kullanilir."""
        if self.use_ssl:
            ctx = ssl.create_default_context()
            s = smtplib.SMTP_SSL(self.host, self.port, timeout=self.timeout, context=ctx)
        else:
            s = smtplib.SMTP(self.host, self.port, timeout=self.timeout)
            s.ehlo()
            if self.use_tls:
                ctx = ssl.create_default_context()
                s.starttls(context=ctx)
                s.ehlo()
        if self.username and self.password:
            s.login(self.username, self.password)
        return s

    def test_connection(self) -> dict:
        """SMTP'ye bagla, login ol, NOOP at, disconnect. Mail gondermez."""
        if not self.available:
            return {"ok": False, "error": self._init_error}
        try:
            with self._connect() as s:
                status, _ = s.noop()
                return {"ok": True, "status_code": status, "host": self.host, "port": self.port}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ──────────────────────────────────────────────
    # Sablon + gonderim
    # ──────────────────────────────────────────────

    def _render_html(self, username: str, display_name: Optional[str],
                     score_result: dict) -> str:
        score = score_result.get("score", 0)
        grade = score_result.get("grade", "A")
        grade_colors = {"A": "#16a34a", "B": "#65a30d", "C": "#d97706",
                         "D": "#dc2626", "E": "#991b1b"}
        grade_color = grade_colors.get(grade, "#64748b")

        # HTML escaping: label/suggestions kullaniciya ait metadata'dan gelebiliyor
        # (username, display_name, file_name tabanli iceriği yok bu uretimde ama
        # yine de defense-in-depth — email client'i stripse bile iyi hijyen).
        esc = html.escape
        factors_rows = ""
        for f in score_result.get("factors", []):
            factors_rows += (
                f'<tr>'
                f'<td style="padding:8px;border-bottom:1px solid #e5e7eb">{esc(str(f.get("label", "")))}</td>'
                f'<td style="padding:8px;border-bottom:1px solid #e5e7eb;text-align:right">{int(f.get("count", 0) or 0)}</td>'
                f'<td style="padding:8px;border-bottom:1px solid #e5e7eb;text-align:right;color:#dc2626;font-weight:600">-{int(f.get("penalty", 0) or 0)}</td>'
                f'</tr>'
            )
        if not factors_rows:
            factors_rows = (
                '<tr><td colspan="3" style="padding:12px;text-align:center;color:#64748b">'
                'Hicbir uyumsuzluk tespit edilmedi. Iyi gidiyorsunuz.</td></tr>'
            )

        suggestions = "".join(
            f'<li style="margin-bottom:8px">{esc(str(s))}</li>'
            for s in score_result.get("suggestions", [])
        ) or '<li style="color:#64748b">Herhangi bir oneri yok.</li>'

        greeting = f"Merhaba {esc(display_name or username)},"

        return f"""<!DOCTYPE html>
<html lang="tr"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Dosya Verimlilik Raporunuz</title></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#1f2937">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f3f4f6;padding:24px 0">
<tr><td align="center">
<table role="presentation" width="600" cellspacing="0" cellpadding="0" style="max-width:600px;width:100%;background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1)">
  <tr><td style="padding:24px;background:linear-gradient(135deg,#3b82f6,#6366f1);color:#ffffff">
    <div style="font-size:12px;text-transform:uppercase;letter-spacing:1px;opacity:0.85">FILE ACTIVITY</div>
    <div style="font-size:22px;font-weight:700;margin-top:4px">Dosya Verimlilik Raporunuz</div>
  </td></tr>
  <tr><td style="padding:24px">
    <p style="margin:0 0 16px 0;font-size:15px">{greeting}</p>
    <p style="margin:0 0 24px 0;font-size:14px;line-height:1.6;color:#4b5563">
      Son tarama sonucunuza gore kisisel dosya verimlilik skorunuz asagidadir.
      Skor, arsivlemeye uygun eski dosyalar, buyuk boyutlu dosyalar, kopyalar
      ve adlandirma standartlarina uyumlulugu birlikte degerlendirir.
    </p>

    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin:0 0 24px 0">
      <tr>
        <td align="center" style="padding:20px;background:#f9fafb;border-radius:8px">
          <div style="font-size:48px;font-weight:700;color:{grade_color};line-height:1">{score}</div>
          <div style="font-size:12px;color:#6b7280;margin-top:4px">/ 100 puan</div>
          <div style="margin-top:12px;font-size:24px;font-weight:700;color:{grade_color}">Sinif: {grade}</div>
        </td>
      </tr>
    </table>

    <h3 style="font-size:14px;margin:0 0 8px 0;color:#374151;text-transform:uppercase;letter-spacing:0.5px">Faktorler</h3>
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="font-size:13px;border-collapse:collapse;margin-bottom:24px">
      <thead>
        <tr style="background:#f3f4f6">
          <th style="padding:8px;text-align:left;font-weight:600;font-size:11px;text-transform:uppercase;color:#6b7280">Kategori</th>
          <th style="padding:8px;text-align:right;font-weight:600;font-size:11px;text-transform:uppercase;color:#6b7280">Adet</th>
          <th style="padding:8px;text-align:right;font-weight:600;font-size:11px;text-transform:uppercase;color:#6b7280">Puan</th>
        </tr>
      </thead>
      <tbody>{factors_rows}</tbody>
    </table>

    <h3 style="font-size:14px;margin:0 0 8px 0;color:#374151;text-transform:uppercase;letter-spacing:0.5px">Oneriler</h3>
    <ul style="font-size:14px;line-height:1.6;padding-left:20px;margin:0 0 24px 0;color:#1f2937">{suggestions}</ul>

    <p style="margin:24px 0 0 0;font-size:12px;color:#6b7280;border-top:1px solid #e5e7eb;padding-top:16px">
      Bu rapor otomatik olarak olusturulmustur. Sorulariniz icin sistem yoneticinize basvurun.
    </p>
  </td></tr>
</table>
</td></tr></table>
</body></html>"""

    def _render_text(self, username: str, display_name: Optional[str],
                     score_result: dict) -> str:
        lines = [
            f"Merhaba {display_name or username},",
            "",
            f"Dosya verimlilik skorunuz: {score_result.get('score', 0)}/100 "
            f"(Sinif: {score_result.get('grade', 'A')})",
            "",
            "Faktorler:",
        ]
        for f in score_result.get("factors", []) or []:
            lines.append(f"  - {f.get('label', '')}: {f.get('count', 0)} adet (ceza: -{f.get('penalty', 0)})")
        if not score_result.get("factors"):
            lines.append("  (uyumsuzluk tespit edilmedi)")
        lines.append("")
        lines.append("Oneriler:")
        for s in score_result.get("suggestions", []) or []:
            lines.append(f"  - {s}")
        lines.append("")
        lines.append("-- FILE ACTIVITY (otomatik bildirim)")
        return "\n".join(lines)

    def _build_message(self, username: str, email: str, score_result: dict,
                        display_name: Optional[str]) -> tuple:
        """Subject + MIMEMultipart + recipient list hazirla."""
        score = score_result.get("score", 0)
        grade = score_result.get("grade", "A")
        subject = f"{self.subject_prefix} Dosya Verimlilik Raporu — {grade} ({score}/100)"

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = formataddr((self.from_name, self.from_address))
        msg["To"] = email
        recipients = [email]
        if self.admin_cc and self.admin_cc.lower() != email.lower():
            msg["Cc"] = self.admin_cc
            recipients.append(self.admin_cc)

        text = self._render_text(username, display_name, score_result)
        html = self._render_html(username, display_name, score_result)
        msg.attach(MIMEText(text, "plain", "utf-8"))
        msg.attach(MIMEText(html, "html", "utf-8"))
        return subject, msg, recipients

    def send_user_report(self, username: str, email: str,
                          score_result: dict,
                          display_name: Optional[str] = None,
                          smtp_session: Optional[smtplib.SMTP] = None) -> dict:
        """Kullaniciya verimlilik raporu e-postasi gonder.

        admin_cc varsa CC'ye eklenir. Gonderim sonucu notification_log'a yazilir.

        smtp_session: opsiyonel. Batch cagrilar icin EmailNotifier.session()
        context manager'indan alinan acik bir SMTP baglantisi verilirse her
        gonderimde yeni baglanti acilmaz. None ise her cagri icin kendi
        baglantisini acar ve kapatir.
        """
        if not self.available:
            return {"ok": False, "error": self._init_error, "skipped": True}
        if not email:
            return {"ok": False, "error": "hedef e-posta yok", "skipped": True}

        subject, msg, recipients = self._build_message(username, email,
                                                         score_result, display_name)
        try:
            if smtp_session is not None:
                smtp_session.sendmail(self.from_address, recipients, msg.as_string())
            else:
                with self._connect() as s:
                    s.sendmail(self.from_address, recipients, msg.as_string())
            self._log(username, email, subject, "sent", score_result=score_result,
                       cc=self.admin_cc or None)
            return {"ok": True, "email": email, "cc": self.admin_cc or None, "subject": subject}
        except Exception as e:
            err = str(e)
            logger.warning("E-posta gonderilemedi (%s): %s", email, err)
            self._log(username, email, subject, "error", error=err,
                       cc=self.admin_cc or None)
            return {"ok": False, "error": err}

    @contextmanager
    def session(self):
        """Batch gonderim icin persistent SMTP oturumu aciklama context manager'i.

        Kullanim:
            with notifier.session() as smtp:
                for user in users:
                    notifier.send_user_report(..., smtp_session=smtp)

        Bir kez baglanir, tum gonderimler ayni soket uzerinden gider. Office
        365 gibi rate-limited sunucularda bagimsiz baglanti sayisini makul
        tutar. available=False ise hata atmaz, smtp=None yield eder ve
        send_user_report her cagrida kendi baglantisini acar (downgrade).
        """
        if not self.available:
            yield None
            return
        smtp = None
        try:
            smtp = self._connect()
            yield smtp
        finally:
            if smtp is not None:
                try:
                    smtp.quit()
                except Exception:
                    pass

    def _log(self, username: str, email: str, subject: str, status: str,
              error: Optional[str] = None, cc: Optional[str] = None,
              score_result: Optional[dict] = None):
        try:
            payload = None
            if score_result is not None:
                # Kucuk bir ozet log'a, tam score_result yerine
                payload = json.dumps({
                    "score": score_result.get("score"),
                    "grade": score_result.get("grade"),
                    "non_compliance": score_result.get("non_compliance"),
                }, ensure_ascii=False)
            with self.db.get_cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO notification_log
                    (username, email, cc, subject, status, error, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (username, email, cc, subject, status, error, payload),
                )
        except Exception as e:
            logger.warning("notification_log yazilamadi: %s", e)

    def health(self) -> dict:
        info = {
            "enabled": self.enabled,
            "available": self.available,
            "host": self.host or None,
            "port": self.port,
            "use_tls": self.use_tls,
            "use_ssl": self.use_ssl,
            "from_address": self.from_address or None,
            "admin_cc_email": self.admin_cc or None,
        }
        if self._init_error:
            info["init_error"] = self._init_error
        return info
