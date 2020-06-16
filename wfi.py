from utilities import driver_path, today, ltd, waf, mtf, IP
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import random

source = {'SINA':{'site':'http://finance.sina.com.cn/realstock/company/sh000001/nc.shtml'}, 'NIKKEI':{'site':'https://indexes.nikkei.co.jp/en/nkave/index/profile?idx=nk225', 'delta-id':'diff'}, 'CNBC':{'site':'https://www.cnbc.com/pre-markets/', 'delta-xpath':'BasicTable-quote'}, 'WhatsApp':{'site':'https://web.whatsapp.com'}, 'SMS':{'site':'https://messages.google.com/web'}}
idxfs = ['DOW', 'S&P', 'Nasdaq', 'Russell']
fields = ['open','high','low','close','volume']
lf, preference = waf(), 'Firefox'
if today.day == ltd(today.year, today.month): lf = waf(1)

def fstrip(_, sep=','):
    return float(''.join(_.split(sep)))

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
        self.browser.execute_script(f"window.open('{source['WhatsApp']['site']}','WhatsApp');")
        self.browser.execute_script(f"window.open('{source['CNBC']['site']}','CNBC');")
        self.browser.execute_script(f"window.open('{source['NIKKEI']['site']}','NIKKEI');")
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
        if tab in __:
            self.__update(tab.upper(), 'close', close)
            self.__update(tab.upper(), 'volume', volume)
            self.__cfm([tab.upper()])

    def usif(self, idx='DOW', site='CNBC'):
        if self.browser.current_url == source[site]['site']: self.refresh(site)
        else: self.browser.switch_to.window(site)
        _ = [__.text for __ in self.browser.find_elements_by_xpath(f"//td[@class='{source[site]['delta-xpath']}Gain' or @class='{source[site]['delta-xpath']}Decline']")]
        if idx in idxfs:
            ix = 2 * idxfs.index(idx)
            return [fstrip(__) for __ in _[ix:ix+2]]

    def nk225(self, site='NIKKEI'):
        if self.browser.current_url == source[site]['site']: self.refresh(site)
        else: self.browser.switch_to.window(site)
        _ = self.browser.find_element_by_id(source[site]['delta-id'])
        t = _.text.split(' ')[0].split(',')
        return fstrip(t)

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
