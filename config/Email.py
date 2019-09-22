# coding=utf-8

import re
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication


class Email:

    def __init__(self, receivers, mail_subject, mail_content, attachment_file="", image_paths=[], cc_users=None):
        """
        :param receivers: list/收件人组
        :param mail_subject: string/邮件主题
        :param mail_content: string/邮件正文
        :param attachment_file: string or list/邮件附件
        :param image_paths: list[string]/正文图片组地址
        :param cc_users: list/抄送人组
        :return:
        """
        self.sender = 'ernestw4q12@163.com'
        self.smtp_server = 'smtp.{0}.com'.format(re.findall(r"@(.*)\.", self.sender)[0])
        self.username = 'ernestw4q12@163.com'
        self.password = ''
        if type(receivers) is str:
            receivers = [receivers]
        self.receivers = receivers
        self.cc_users = cc_users
        self.mail_subject = mail_subject
        # 确定正文文本格式
        if re.search("<.*>.*</.*>", mail_content) is not None:
            self._subtype = 'html'
        else:
            self._subtype = 'plain'
        self.mail_content = MIMEText(_text=mail_content, _subtype=self._subtype, _charset='utf-8')
        self.attachment_file = attachment_file
        self.image_paths = image_paths

    # 附件信息
    def mail_attachment(self, file_info):
        if self.attachment_file != "":
            attachment = MIMEApplication(open(file_info, 'rb+').read())
            attachment.add_header('Content-Disposition', 'attachment', filename=os.path.split(file_info)[1])
            return attachment
        else:
            pass

    # 正文图片
    def mail_image(self, image_path):
        # 读取图片并添加邮件头
        image_file = open(image_path, 'rb')
        image = MIMEImage(image_file.read())
        image_file.close()
        image.add_header('Content-ID', '<image{0}>'.format(image_path))
        return image

    # 邮件正文（正文,附件,图片）
    def mail_text(self):
        text = MIMEMultipart()
        text['from'] = self.sender
        text['to'] = ';'.join(self.receivers)
        if self.cc_users is None:
            pass
        else:
            text['cc'] = ';'.join(self.cc_users)
        text['subject'] = self.mail_subject
        text.attach(self.mail_content)
        if self.attachment_file != "" or self.attachment_file is list:
            if type(self.attachment_file) is str:
                self.attachment_file = [self.attachment_file]
            for attachment_file_value in self.attachment_file:
                text.attach(self.mail_attachment(attachment_file_value))
        if self._subtype == 'html' and len(self.image_paths) > 0:
            for image_path in self.image_paths:
                text.attach(self.mail_image(image_path))
        return text

    # 邮件发送
    def send(self):
        # 声明邮件对象并连接服务器
        # smtp = smtplib.SMTP_SSL(self.smtp_server, 465)
        smtp = smtplib.SMTP()
        smtp.connect(self.smtp_server, 25)
        # 打开安全通信协议（TLS）
        # smtp.starttls()
        # 登录信息
        smtp.login(self.username, self.password)
        # 邮件内容
        smtp.sendmail(self.sender, self.receivers, self.mail_text().as_string())
        # 断开连接
        smtp.quit()






