from utilities import driver_path, today, ltd, waf, mtf, IP
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import random

sites = {'SINA':'http://finance.sina.com.cn/realstock/company/sh000001/nc.shtml', 'NIKKEI':'https://indexes.nikkei.co.jp/en/nkave', 'CNBC_Pre':'https://www.cnbc.com/pre-markets/', 'WhatsApp':'https://web.whatsapp.com', 'SMS':'https://messages.google.com/web'}
fields = ['open','high','low','close','volume']
lf, preference = waf(), 'Firefox'
if today.day == ltd(today.year, today.month): lf = waf(1)

class WFutures(object):
    def __init__(self, ip=None, _=None):
        if _ is None: _ = preference
        self.lip = ip
        if ip is None: self.lip = str(IP())
        self.browser = eval(f"webdriver.{_}(executable_path=driver_path('{_}'))")
        wait = WebDriverWait(self.browser, 600)
        self.browser.get(f'http://{self.lip}/futures')
        self.window0 = self.browser.window_handles[0]
        self.browser.execute_script(f"window.open('http://{self.lip}/equities','Local');")
        self.refresh(self.window0)
        self.__load(lf)
        self.browser.execute_script(f"window.open('{sites['WhatsApp']}','WhatsApp');")
        self.browser.execute_script(f"window.open('{sites['CNBC_Pre']}','CNBC');")
        self.browser.switch_to.window(self.window0)

    def __del__(self):
        self.lip = self.browser = self.pivot = self.eb = None
        del self.lip, self.browser, self.pivot, self.eb

    def kill(self):
        self.browser.quit()
        del self.browser

    def close_down(self, tab, close, volume):
        __ = list(lf)
        __.extend([x.lower() for x in lf])
        # if tab in lf:
        if tab in __:
            self.__update(tab.upper(), 'close', close)
            self.__update(tab.upper(), 'volume', volume)
            self.__cfm([tab.upper()])

    def dows(self, site='CNBC'):
        self.browser.switch_to.window(site)
        return [_.text for _ in self.browser.find_elements_by_class_name('BasicTable-quoteGain')][:2]

    def reset(self, tabs=lf):
        for _ in [__ for __ in tabs if __ in lf]:
            self.browser.switch_to.window(_)
            self.browser.back()
            self.refresh(_)
        if tabs in [self.window0, 'Local']:
            self.browser.switch_to.window(tabs)
            self.browser.back()
            self.refresh(tabs)

    def set_open(self, tab, _):
        __ = list(lf)
        __.extend([x.lower() for x in lf])
        if tab in __:
            self.__update(tab.upper(), 'open', _)
        if tab == self.window0:
            self.browser.switch_to.window(tab)
            self.pivot.clear()
            self.pivot.send_keys(_)

    def __update(self, tab, field, _):
        if (tab in lf) and (field in fields):
            self.browser.switch_to.window(tab)
            t = (tab[0] + tab[-2] + field[0]).lower()
            exec(f'self.{t}.clear()')
            exec(f'self.{t}.send_keys({_})')

    def update_high(self, tab, _):
        __ = list(lf)
        __.extend([x.lower() for x in lf])
        if tab in __: self.__update(tab.upper(), 'high', _)

    def update_low(self, tab, _):
        __ = list(lf)
        __.extend([x.lower() for x in lf])
        if tab in __: self.__update(tab.upper(), 'low', _)

    def __cfm(self, tabs=lf):
        for _ in [__ for __ in tabs if __ in lf]:
            self.browser.switch_to.window(_)
            t = (_[0] + _[-2] + 'b').lower()
            exec(f"self.{t}.click()")
        if tabs == self.window0:
            self.browser.switch_to.window(tabs)
            self.eb.click()

    def refresh(self, _):
        if _ in lf:
            self.browser.switch_to.window(_)
            self.browser.back()
            for __ in self.browser.find_elements_by_tag_name('option'):
                if __.text == _:
                    __.click()
                    break
            t = (_[0] + _[-2] + 'b').lower()
            exec(f"self.{t}=self.browser.find_element_by_tag_name('button')")
            for __ in fields:
                t = (_[0] + _[-2] + __[0]).lower()
                exec(f"self.{t}=self.browser.find_element_by_name('{__}')")
                exec(f"self.{t}.clear()")
        if _ == self.window0:
            self.browser.switch_to.window(_)
            for __ in self.browser.find_elements_by_tag_name('option'):
                if __.text == mtf('mhi'):
                    __.click()
                    break
            self.pivot = self.browser.find_element_by_name('pp')
            self.pivot.clear()
            self.eb = self.browser.find_element_by_tag_name('button')
        if _ == 'Local':
            self.browser.switch_to.window(_)
            options = self.browser.find_elements_by_tag_name('option')
            preferred_code = random.choice([__.text for __ in options])
            for __ in options:
                if __.text == preferred_code:
                    __.click()
                    break
            self.eb = self.browser.find_element_by_tag_name('button')

    def analyse(self, code):
        if isinstance(code, (int, float)): code = str(int(code))
        self.reset('Local')
        # self.browser.switch_to.window('Local')
        # self.browser.back()
        for _ in self.browser.find_elements_by_tag_name('option'):
            if _.text == code:
                _.click()
                break
        self.browser.find_element_by_tag_name('button').click()

    def __load(self, tabs):
        for _ in [__ for __ in tabs if __ in lf]:
            self.browser.execute_script(f"window.open('http://{self.lip}','{_}');")
            # fields = ['open','high','low','close','volume']
            self.browser.switch_to.window(_)
            for __ in self.browser.find_elements_by_tag_name('option'):
                if __.text == _:
                    __.click()
                    break
            t = (_[0] + _[-2] + 'b').lower()
            exec(f"self.{t}=self.browser.find_element_by_tag_name('button')")
            for __ in fields:
                t = (_[0] + _[-2] + __[0]).lower()
                exec(f"self.{t}=self.browser.find_element_by_name('{__}')")
