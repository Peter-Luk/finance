from utilities import driver_path, today, ltd, waf, mtf, IP, datetime
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import re, random

from pref import source, fields
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
        # self.auxiliary_load(['NIKKEI', 'CNBC', 'SINA', 'WhatsApp'])
        self.__load(lf)
        self.refresh(self.window0)

    def __del__(self):
        self.lip = self.browser = self.pivot = self.eb = None
        del self.lip, self.browser, self.pivot, self.eb

    def auxiliary_load(self, _=['WhatsApp', 'CNBC', 'NIKKEI', 'SINA']):
        if not isinstance(_, (list, tuple)): _ = [_]
        [self.browser.execute_script(f"window.open('{source[__]}', '{__}');") for __ in _ if __ in source.keys()]

    def kill(self):
        self.browser.quit()
        del self.browser

    def close_down(self, tab, close, volume):
        tab = tab.upper()
        if tab in lf:
            self.__update(tab, 'close', close)
            self.__update(tab, 'volume', volume)
            self.__cfm([tab])

    def goto(self, _):
        self.browser.switch_to.window(_)

    def shanghai_composite(self, site='SINA'):
        if self.browser.current_url == source[site]: self.refresh(site)
        else: self.goto(site)
        price = self.browser.find_element_by_xpath('//*[@id="price"]').text
        change = self.browser.find_element_by_xpath('//*[@id="change"]').text
        last = datetime.strptime(self.browser.find_element_by_xpath('//*[@id="hqTime"]').text, '%Y-%m-%d %H:%M:%S')
        if change == '--': change = '0'
        _ = [float(_.replace(',','')) for _ in [price, change]]
        __ = _[-1] / (_[0] - _[-1]) * 100
        __ = float(f'{__:0.3f}')
        _.extend([__, last])
        return _

    def load_A_share(self, code, site='SINA'):
        if isinstance(code, int): code = f'{code:06}'
        __ = source[site].replace('000001', code)
        self.browser.execute_script(f"window.open('{__}', 'sh{code}');")
        self.goto(f'sh{code}')

    def shanghai_A(self, code, site='SINA'):
        if isinstance(code, int): code = f'{code:06}'
        __ = source[site].replace('000001', code)
        if self.browser.current_url == __: self.refresh(f'sh{code}')
        else: self.goto(f'sh{code}')
        price = self.browser.find_element_by_xpath('//*[@id="price"]').text
        change = self.browser.find_element_by_xpath('//*[@id="change"]').text
        last = datetime.strptime(self.browser.find_element_by_xpath('//*[@id="hqTime"]').text, '%Y-%m-%d %H:%M:%S')
        if change == '--': change = '0'
        _ = [float(_.replace(',','')) for _ in [price, change]]
        __ = _[-1] / (_[0] - _[-1]) * 100
        __ = float(f'{__:0.3f}')
        _.extend([__, last])
        return _

    def usif(self, idx='Dow', site='CNBC', implied=True):
        if self.browser.current_url == source[site]: self.refresh(site)
        else: self.goto(site)
        div = 'div[2]'
        if  implied: div = 'div[4]'

        def cxpath(_):
            idx = ['Dow', 'S&P', 'Nasdaq', 'Russell']
            if _ in idx:
                return f'/html/body/div[2]/div[2]/div[1]/div[3]/div[2]/div/div/div[3]/div[1]/div/div[1]/div[{1+idx.index(_)}]/div'

        _ = self.browser.find_element_by_xpath(cxpath(idx))
        price = _.find_element_by_xpath(f'./{div}/div/div/table/tbody/tr/td[2]').text
        change = _.find_element_by_xpath(f'./{div}/div/div/table/tbody/tr/td[3]').text
        last = datetime.strptime(''.join(re.split('\: |\|', _.find_element_by_xpath('./div[5]').text)[1:]), '%a %b %d %Y %I:%M %p EDT')
        _ = [float(__.replace(',','')) for __ in [price, change]]
        __ = _[-1] / (_[0] - _[-1]) * 100
        __ = float(f'{__:0.3f}')
        _.extend([__, last])
        return _

    def nk225(self, site='NIKKEI'):
        if self.browser.current_url == source[site]: self.refresh(site)
        else: self.goto(site)
        def convert(_):
            rstring  = '\([0-2][0-9]\:[0-5][0-9]\)'
            if re.search(rstring, _):
                return datetime.strptime(_, '%b/%d/%Y(%H:%M)')
            return datetime.strptime(_.split('(')[0], '%b/%d/%Y')

        price = self.browser.find_element_by_xpath('//*[@id="price"]').text
        change = self.browser.find_element_by_xpath('//*[@id="diff"]').text
        t = change.split(' ')[0].split(',')
        last = convert(self.browser.find_element_by_xpath('//*[@id="datedtime"]').text)
        _ = [float(__.replace(',','')) for __ in [price, t[0]]]
        __ = _[-1] / (_[0] - _[-1]) * 100
        __ = float(f'{__:0.3f}')
        _.extend([__, last])
        # _.append(last)
        return _

    def reset(self, tabs=lf):
        for _ in [__ for __ in tabs if __ in lf]:
            self.goto(_)
            self.browser.back()
            self.refresh(_)
        if tabs in [self.window0, 'Local']:
            self.goto(tabs)
            self.browser.back()
            self.refresh(tabs)

    def set_open(self, tab, _):
        if tab.upper() in lf: self.__update(tab.upper(), 'open', _)
        if tab == self.window0:
            self.goto(tab)
            self.pivot.clear()
            self.pivot.send_keys(_)

    def __update(self, tab, field, _):
        if (tab in lf) and (field in fields):
            self.goto(tab)
            t = (tab[0] + tab[-2] + field[0]).lower()
            exec(f'self.{t}.clear()')
            exec(f'self.{t}.send_keys({_})')

    def update_high(self, tab, _):
        tab = tab.upper()
        if tab in lf: self.__update(tab, 'high', _)

    def update_low(self, tab, _):
        tab = tab.upper()
        if tab in lf: self.__update(tab, 'low', _)

    def __cfm(self, tabs=lf):
        for _ in [__ for __ in tabs if __ in lf]:
            self.goto(_)
            t = (_[0] + _[-2] + 'b').lower()
            exec(f"self.{t}.click()")
        if tabs == self.window0:
            self.goto(tabs)
            self.eb.click()

    def refresh(self, _):
        if _.upper() in lf:
            self.goto(_.upper())
            self.browser.back()
            for __ in self.browser.find_elements_by_tag_name('option'):
                if __.text == _.upper():
                    __.click()
                    break
            t = (_[0] + _[-2] + 'b').lower()
            exec(f"self.{t}=self.browser.find_element_by_tag_name('button')")
            for __ in fields:
                t = (_[0] + _[-2] + __[0]).lower()
                exec(f"self.{t}=self.browser.find_element_by_name('{__}')")
                exec(f"self.{t}.clear()")
        if _ == self.window0:
            self.goto(_)
            for __ in self.browser.find_elements_by_tag_name('option'):
                if __.text == mtf('mhi'):
                    __.click()
                    break
            self.pivot = self.browser.find_element_by_name('pp')
            self.pivot.clear()
            self.eb = self.browser.find_element_by_tag_name('button')
        if _ == 'Local':
            self.goto(_)
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
        for _ in self.browser.find_elements_by_tag_name('option'):
            if _.text == code:
                _.click()
                break
        self.browser.find_element_by_tag_name('button').click()

    def __load(self, tabs):
        for _ in [__ for __ in tabs if __ in lf]:
            self.browser.execute_script(f"window.open('http://{self.lip}','{_}');")
            self.goto(_)
            for __ in self.browser.find_elements_by_tag_name('option'):
                if __.text == _:
                    __.click()
                    break
            t = (_[0] + _[-2] + 'b').lower()
            exec(f"self.{t}=self.browser.find_element_by_tag_name('button')")
            for __ in fields:
                t = (_[0] + _[-2] + __[0]).lower()
                exec(f"self.{t}=self.browser.find_element_by_name('{__}')")
