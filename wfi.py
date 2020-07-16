from utilities import driver_path, today, ltd, waf, mtf, IP
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import random

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
        self.auxiliary_load(['NIKKEI', 'CNBC', 'WhatsApp'])
        self.__load(lf)
        self.refresh(self.window0)

    def __del__(self):
        self.lip = self.browser = self.pivot = self.eb = None
        del self.lip, self.browser, self.pivot, self.eb

    def auxiliary_load(self, _):
        if not isinstance(_, (list, tuple)): _ = [_]
        [self.browser.execute_script(f"window.open('{source[__]['site']}', '{__}');") for __ in _ if __ in source.keys()]

    def cxpath(_, implied=True):
        idx, div = ['Dow', 'S&P', 'Nasdaq', 'Russell'],'div[2]'
        if _ in idx:
            if implied: div = 'div[4]'
            return f'/html/body/div[2]/div[2]/div[1]/div[3]/div[2]/div/div/div[3]/div[1]/div/div[{1+idx.index(_)}]/div[1]/div/{div}/div/div/table/tr/td[3]'

    def kill(self):
        self.browser.quit()
        del self.browser

    def close_down(self, tab, close, volume):
        tab = tab.upper()
        if tab in lf:
            self.__update(tab, 'close', close)
            self.__update(tab, 'volume', volume)
            self.__cfm([tab])

    def usif(self, idx='Dow', site='CNBC'):
        if self.browser.current_url == source[site]['site']: self.refresh(site)
        else: self.browser.switch_to.window(site)
        divs = self.browser.find_elements_by_tag_name('div')
        divs.reverse()
        def _findidx(idx):
            for __ in divs:
                try:
                    __.find_element_by_partial_link_text(idx)
                    return __
                except: pass
        return [float(__.text.replace(',','')) for __ in _findidx(idx).find_elements_by_xpath(f".//td[@class='{source[site]['delta_xpath']}Gain' or @class='{source[site]['delta_xpath']}Decline']")]

    def nk225(self, site='NIKKEI'):
        if self.browser.current_url == source[site]['site']: self.refresh(site)
        else: self.browser.switch_to.window(site)
        _ = self.browser.find_element_by_id(source[site]['delta_id'])
        t = _.text.split(' ')[0].split(',')
        return float(t[0].replace(',',''))

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
        if tab.upper() in lf: self.__update(tab.upper(), 'open', _)
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
        tab = tab.upper()
        if tab in lf: self.__update(tab, 'high', _)

    def update_low(self, tab, _):
        tab = tab.upper()
        if tab in lf: self.__update(tab, 'low', _)

    def __cfm(self, tabs=lf):
        for _ in [__ for __ in tabs if __ in lf]:
            self.browser.switch_to.window(_)
            t = (_[0] + _[-2] + 'b').lower()
            exec(f"self.{t}.click()")
        if tabs == self.window0:
            self.browser.switch_to.window(tabs)
            self.eb.click()

    def refresh(self, _):
        if _.upper() in lf:
            self.browser.switch_to.window(_.upper())
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
        for _ in self.browser.find_elements_by_tag_name('option'):
            if _.text == code:
                _.click()
                break
        self.browser.find_element_by_tag_name('button').click()

    def __load(self, tabs):
        for _ in [__ for __ in tabs if __ in lf]:
            self.browser.execute_script(f"window.open('http://{self.lip}','{_}');")
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
