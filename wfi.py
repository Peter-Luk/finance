from utilities import driver_path, waf, mtf, IP
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
preference = 'Firefox'

class WFutures(object):
    def __init__(self, ip=None, _=None):
        if _ is None: _ = preference
        self.lip = ip
        if ip is None: self.lip = str(IP())
        self.browser = eval(f"webdriver.{_}(executable_path=driver_path('{_}'))")
        wait = WebDriverWait(self.browser, 600)
        self.browser.get(f'http://{self.lip}/estimate')
        self.window0 = self.browser.window_handles[0]
        self.refresh(self.window0)
        self.__load(waf())
        self.browser.switch_to.window(self.window0)

    def __del__(self):
        self.lip = self.browser = self.pivot = self.eb = None
        del self.lip, self.browser, self.pivot, self.eb

    def kill(self):
        self.browser.quit()
        del self.browser

    def refresh(self, _):
        if _ in waf():
            fields = ['open','high','low','close','volume']
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
                exec(f"self.{t}.clear()")
        elif _ == self.window0:
            self.browser.switch_to.window(_)
            for __ in self.browser.find_elements_by_tag_name('option'):
                if __.text == mtf('mhi'):
                    __.click()
                    break
            self.pivot = self.browser.find_element_by_name('pp')
            self.pivot.clear()
            self.eb = self.browser.find_element_by_tag_name('button')

    def __load(self, tabs):
        for _ in [__ for __ in tabs if __ in waf()]:
            self.browser.execute_script(f"window.open('http://{self.lip}','{_}');")
            self.refresh(_)
