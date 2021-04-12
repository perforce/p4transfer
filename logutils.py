#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
# logutils.py - Utilities for logging etc
#
#####################################################
# OVERVIEW:
#
# Logging utilities
#   For enhancement ideas, see http://astropy.readthedocs.org/en/latest/logging.html
######################################################
"""

import sys
import os
import time
import logging
import smtplib
import requests
from email.mime.text import MIMEText

python3 = sys.version_info[0] >= 3

if python3:
    import urllib.parse
    import urllib.request
else:
    import urllib


def notify_users_by_email(mail_from, mail_to, mail_server, subject, body):
    "Uses simple form to send an email to fixed set of users"
    if not mail_to or not mail_from or not mail_server:
        return
    try:
        smtp = smtplib.SMTP(mail_server)
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = mail_from
        msg['To'] = mail_to
        smtp.sendmail(mail_from, mail_to.split(","), msg.as_string())
        smtp.quit()
    except Exception as e:
        print("Failed to send mail:", str(e))
        sys.stdout.flush()


def notify_users_by_form(mail_form_url, subject, msg):
    "Uses simple form to send an email to fixed set of users"
    if not mail_form_url:
        return
    response = None
    try:
        if "api" in mail_form_url:
            # Mailgun API
            requests.post("%s/messages" % mail_form_url["url"],
                          auth=("api", mail_form_url["api"]),
                          data={"from": mail_form_url["mail_from"],
                                "to": mail_form_url["mail_to"],
                                "subject": subject,
                                "text": msg})
        else:
            if python3:
                data = urllib.parse.urlencode({'subject': subject, 'message': msg})
                response = urllib.request.urlopen(mail_form_url, data.encode('ascii'))
            else:
                data = urllib.urlencode({'subject': subject, 'message': msg})
                response = urllib.urlopen(mail_form_url, data)
    except Exception as e:
        print("Failed to notify:", str(e))
        sys.stdout.flush()
    return response


def save_existing_file(file_name):
    "Ensures we don't overwrite existing files by renaming them"
    if os.path.exists(file_name):
        new_name = os.path.basename(file_name)
        (new_name, ext) = new_name.rsplit(".", 1)
        new_name = "%s-%s.%s" % (
            new_name, time.strftime("%Y%m%d%H%M%S", time.localtime()), ext)
        os.rename(file_name, new_name)
        return new_name
    return None


def get_unique_file_name(file_path):
    "Ensures we have a unique file name"
    i = 1
    ldir = os.path.dirname(file_path)
    (base_file_name, ext) = os.path.basename(file_path).rsplit(".", 1)
    while os.path.exists(file_path):
        file_path = os.path.join(ldir, "%s-%d.%s" % (base_file_name, i, ext))
        i += 1
    return file_path


def get_log_file_name():
    prefix = sys.argv[0].split(".py")[0]
    return get_unique_file_name(os.path.join(
        os.getcwd(), "log-%s-%s.log" % (os.path.basename(prefix),
                                        time.strftime('%Y%m%d%H%M%S', time.localtime()))))


class ArgLogRecord(logging.LogRecord):
    """Custom formatting - just prints out any arguments passed"""
    def __init__(self, name, level, pathname, lineno,
                 msg, args, exc_info, func=None, extra=None, sinfo=None):
        if sys.version_info[0] < 3 or \
           (sys.version_info[0] == 3 and sys.version_info[1] < 2):
            logging.LogRecord.__init__(self, name, level, pathname, lineno, msg,
                                       args, exc_info, func)
        else:
            logging.LogRecord.__init__(self, name, level, pathname, lineno, msg,
                                       args, exc_info, func=func, extra=extra,
                                       sinfo=sinfo)

    def getMessage(self):
        """Return the message for this LogRecord.
        Just prints any arguments left over"""
        msg = str(self.msg)
        if self.args:
            try:
                msg = msg % self.args
            except TypeError:
                msg += ", ".join([str(x) for x in self.args])
        return msg


class ArgLogger(logging.getLoggerClass()):
    "Specific logging class to use our logrecord"
    def __init__(self, name, **kwargs):
        logging.Logger.__init__(self, name, **kwargs)
        self.saved_output = []
        self.saved_log = []
        self.mail_form_url = None
        self.mail_to = None
        self.mail_from = None
        self.mail_server = None
        self.report_interval = 30     # minutes
        self.time_last_notified = time.time()

    def setReportingOptions(self, instance_name=None, mail_form_url=None, mail_to=None, mail_from=None, mail_server=None,
                            report_interval=30):
        self.instance_name = instance_name if instance_name else sys.argv[0]
        self.mail_form_url = mail_form_url
        self.mail_to = mail_to
        self.mail_from = mail_from
        self.mail_server = mail_server
        self.report_interval = report_interval

    def _formatRecord(self, record):
        "Find and call formatter"
        for h in self.handlers:
            if h.formatter:
                fmt = h.formatter
            else:
                fmt = logging._defaultFormatter
            return fmt.format(record)

    def _saveRecord(self, record):
        "Save to circular buffers"
        line = self._formatRecord(record)
        if record.levelno == logging.DEBUG:
            self.saved_log.append(line)
            if len(self.saved_log) > 100:
                del self.saved_log[0]
        else:
            self.saved_output.append(line)
            if len(self.saved_output) > 50:
                del self.saved_output[0]
        if time.time() - self.time_last_notified > self.report_interval * 60:
            self.notify("Regular update for %s" % self.instance_name, "")

    def makeRecord(self, name, level, fn, lno, msg, args, exc_info,
                   func=None, extra=None, sinfo=None):
        if sys.version_info[0] < 3 or \
           (sys.version_info[0] == 3 and sys.version_info[1] < 2):
            record = ArgLogRecord(name, level, fn, lno, msg, args, exc_info, func, extra)
        else:
            record = ArgLogRecord(name, level, fn, lno, msg, args, exc_info, func=func, extra=extra, sinfo=sinfo)
        self._saveRecord(record)
        return record

    def report_exception(self):
        "Notify users that we have had a problem"
        self.notify("Exception in %s" % self.instance_name, "", use_log=True)

    def notify(self, subject, body, include_output=True, include_log=False):
        "Notify users of a message"
        self.time_last_notified = time.time()
        subject = "%s: %s" % (self.instance_name, subject)
        body += "\n"
        if include_log:
            body += "\n".join([str(x) for x in self.saved_log])
            self.saved_log = []
        elif include_output:
            body += "\n".join([str(x) for x in self.saved_output])
            self.saved_output = []
        if self.mail_form_url:
            return notify_users_by_form(self.mail_form_url, subject, body)
        else:
            notify_users_by_email(self.mail_from, self.mail_to, self.mail_server, subject, body)


def addFileHandler(logger):
    filename = get_log_file_name()
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s: %(message)s')
    fh = logging.FileHandler(filename=filename)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.info("Logging to file: %s" % filename)


def addStreamHandler(logger, stream):
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s: %(message)s')
    ch = logging.StreamHandler(stream)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def getLogger(logger_name, stream=None):
    if stream is None:
        stream = sys.stdout
    "Register our logger and initialise everything"
    logging.setLoggerClass(ArgLogger)
    logger = logging.getLogger(logger_name)
    if len(logger.handlers) > 0:     # Only set them up once!
        return logger
    addStreamHandler(logger, stream)
    logger.setLevel(logging.DEBUG)
    addFileHandler(logger)
    return logger


def getCurrentLogFileName(logger_name):
    "Get name of current log file"
    logger = logging.getLogger(logger_name)
    for hdlr in logger.handlers:
        if isinstance(hdlr, logging.FileHandler):
            return hdlr.baseFilename


def resetLogger(logger_name):
    "Reset - which creates new files etc"
    logger = logging.getLogger(logger_name)
    for hdlr in logger.handlers:
        if isinstance(hdlr, logging.FileHandler):
            logger.removeHandler(hdlr)
            hdlr.flush()
            hdlr.close()
    addFileHandler(logger)


def resetStreamLogger(logger_name, stream):
    logger = logging.getLogger(logger_name)
    for hdlr in logger.handlers:
        if isinstance(hdlr, logging.StreamHandler):
            logger.removeHandler(hdlr)
            hdlr.flush()
            hdlr.close()
    addStreamHandler(logger, stream)


def test():
    "Test the above"
    logger = getLogger('testlogger')
    logger.debug('debug message')
    logger.info('info message')
    logger.warn('warn message')
    logger.error('error message')
    logger.critical('critical message')

    logger.info("Some", "text", "params")
    logger.info([1, 2, "three", 1.2], "more")
    logger.info("Unicode text", u"file1uåäö")
    logger.info("Unicode text", '\ufffd')
    logging.shutdown()


if __name__ == "__main__":
    test()
