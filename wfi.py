import re
import random
import copy
from utilities import div_input, driver_path, today, ltd, waf, mtf, IP, datetime
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from y2n import Futures
from pref import source, fields, subject, pytz
from pt_2 import festi

local_tz = pytz.timezone('Asia/Hong_Kong')
lf, preference = waf(), 'Firefox'
milly = subject['Milly Ling']['whatsapp']['alias']
if today.day == ltd(today.year, today.month):
    lf = waf(1)


def fan(__='mhi', mt=True):
    _ = [x for x in lf if __.upper() in x and mtf(__) not in x].pop()
    if mt:
        _ = mtf(__)
    _ = Futures(_)
    return f'{_}\n{_()}\n{_.gat()}'


class WFutures(object):
    from dateutil import relativedelta

    def __init__(self, purpose='input', ip=None, _=None):
        if _ is None:
            _ = preference
        self.lip = ip
        if ip is None:
            self.lip = str(IP())
        self.browser = \
            eval(f"webdriver.{_}(executable_path=driver_path('{_}'))")
        self.browser.implicitly_wait(10)
        self.browser.get(f'http://{self.lip}/futures')
        self.wait = WebDriverWait(self.browser, 600)
        self.window0 = self.browser.window_handles[0]
        self.browser.execute_script(f"window.open( \
            'http://{self.lip}/equities','Local');")
        if purpose == 'input':
            self.__load(lf)
        if purpose == 'auxiliary':
            self.auxiliary_load()
        self.refresh(self.window0)

    def __del__(self):
        self.lip = self.browser = self.wait = self.pivot = self.eb = None
        del self.lip, self.browser, self.wait, self.pivot, self.eb

    def __status(self, p, c, last):
        from dateutil.relativedelta import relativedelta
        _ = [round(float(_.replace(',', '')), 1) for _ in [p, c]]
        __ = _[-1] / (_[0] - _[-1]) * 100

        def _dstr(delta):
            ts = []
            tf = ['year', 'month', 'day', 'hour', 'minute', 'second']
            now = datetime.now().astimezone(local_tz)
            r = relativedelta(now, delta)
            for i in tf:
                _t = f'f\'{{r.{i}s}} {i}(s)\''
                ts.append(eval(_t))
            tsd = dict(zip(tf, ts))
            hstr = tsd['year']
            if r.years == 0:
                hstr = tsd['month']
                if r.months == 0:
                    hstr = tsd['day']
                    if r.days == 0:
                        hstr = tsd['hour']
                        if r.hours == 0:
                            hstr = tsd['minute']
                            if r.minutes == 0:
                                hstr = tsd['second']
                            else:
                                hstr += f' {tsd["second"]}'
                        else:
                            hstr += f' {tsd["minute"]} {tsd["second"]}'
                    else:
                        hstr += f' {tsd["hour"]} {tsd["minute"]}'
                        + f' {tsd["second"]}'
                else:
                    hstr += f' {tsd["day"]} {tsd["hour"]}'
                    + f' {tsd["minute"]} {tsd["second"]}'
            else:
                hstr += f' {tsd["month"]} {tsd["day"]}'
                + f' {tsd["hour"]} {tsd["minute"]} {tsd["second"]}'
            return hstr

        l_ = last.astimezone(local_tz)
        _.extend([f'{__:0.3f}%', ' '.join((
            l_.strftime('%b %d, %Y %H:%M:%S'),
            l_.tzname())),
            f'Delay: {_dstr(l_)}'])
        return _

    # def auxiliary_load(self, _=['WhatsApp', 'CNBC', 'NIKKEI', 'SINA', 'Gold']):
    def auxiliary_load(self, _=['CNBC', 'NIKKEI', 'SINA', 'Gold']):
        if not isinstance(_, (list, tuple)):
            _ = [_]
        for __ in _:
            if __ in source.keys():
                hdr = source[__]["hyperlink"]
                self.browser.execute_script(f"window.open('{hdr}', '{__}');")

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
        if self.browser.current_url == source[site]['hyperlink']:
            self.refresh(site)
        else:
            self.goto(site)
        price = self.browser.find_element_by_xpath('//*[@id="price"]').text
        change = self.browser.find_element_by_xpath('//*[@id="change"]').text
        last = source[site]['tz'].localize(datetime.strptime(
            self.browser.find_element_by_xpath('//*[@id="hqTime"]').text,
            '%Y-%m-%d %H:%M:%S'))
        if change == '--':
            change = '0'
        return self.__status(price, change, last)

    def load_A_share(self, code, site='SINA'):
        if isinstance(code, int):
            code = f'{code:06}'
        __ = source[site]['hyperlink'].replace('000001', code)
        self.browser.execute_script(f"window.open('{__}', 'sh{code}');")
        self.goto(f'sh{code}')

    def gold(self, site='Gold'):
        if self.browser.current_url == source[site]['hyperlink']:
            self.refresh(site)
        else:
            self.goto(site)
        try:
            _ = self.browser.find_element_by_xpath(
                '//*[@id="block-wgcheadergoldspotprice"]')
            price = _.find_element_by_xpath('./h2/span[1]').text
            return float(price.replace(',', ''))
        except Exception:
            pass
        return 'Market (probably) close.'

    def shanghai_A(self, code, site='SINA'):
        if isinstance(code, int):
            code = f'{code:06}'
        __ = source[site]['hyperlink'].replace('000001', code)
        if self.browser.current_url == __:
            self.refresh(f'sh{code}')
        else:
            self.goto(f'sh{code}')
        price = self.browser.find_element_by_xpath('//*[@id="price"]').text
        change = self.browser.find_element_by_xpath('//*[@id="change"]').text
        last = source[site]['tz'].localize(datetime.strptime(
            self.browser.find_element_by_xpath('//*[@id="hqTime"]').text,
            '%Y-%m-%d %H:%M:%S'))
        if change == '--':
            change = '0'
        return self.__status(price, change, last)

    def whatsend(self, recipent, message, sender='Peter Luk'):
        try:
            self.goto('WhatsApp')
            x_arg = f'//span[contains(@title, {recipent})]'
            group_title = self.wait.until(EC.presence_of_element_located((
                By.XPATH, x_arg)))
            group_title.click()
            input_box = self.wait.until(EC.presence_of_element_located((
                By.XPATH, f'//div{div_input(sender)}')))
            input_box.send_keys(message + Keys.ENTER)
            return ' @ '.join((
                f'Message successfully sent to {recipent}',
                f'{datetime.now():%H:%M:%S}'))
        except Exception:
            pass

    def usif(self, idx='Dow', site='CNBC', implied=True):
        if self.browser.current_url == source[site]['hyperlink']:
            self.refresh(site)
        else:
            self.goto(site)
        div = 'div[2]'
        if implied:
            div = 'div[4]'

        def cxpath(_):
            idx = ['Dow', 'S&P', 'Nasdaq', 'Russell']
            if _ in idx:
                return f'{source[site]["xpath_base"]}div[{1+idx.index(_)}]/div'

        _ = self.browser.find_element_by_xpath(cxpath(idx))
        price = _.find_element_by_xpath(
            f'./{div}/div/div/table/tbody/tr/td[2]').text
        change = _.find_element_by_xpath(
            f'./{div}/div/div/table/tbody/tr/td[3]').text
        _l = ''.join(re.split('\: |\|', _.find_element_by_xpath(
            './div[5]').text)[1:])
        try:
            last = source[site]['tz'].localize(datetime.strptime(
                _l, '%a %b %d %Y %I:%M %p EST'))
        except Exception:
            last = source[site]['tz'].localize(datetime.strptime(
                _l, '%a %b %d %Y'))
        return self.__status(price, change, last)

    def nk225(self, site='NIKKEI'):
        if self.browser.current_url == source[site]['hyperlink']:
            self.refresh(site)
        else:
            self.goto(site)

        def convert(_):
            rstring = '\([0-2][0-9]\:[0-5][0-9]\)'
            if re.search(rstring, _):
                return source[site]['tz'].localize(
                        datetime.strptime(_, '%b/%d/%Y(%H:%M)'))
            return source[site]['tz'].localize(
                    datetime.strptime(_.split('(')[0], '%b/%d/%Y'))

        price = self.browser.find_element_by_xpath('//*[@id="price"]').text
        change = self.browser.find_element_by_xpath('//*[@id="diff"]').text
        t = change.split(' ')[0].split(',')
        last = convert(self.browser.find_element_by_xpath(
            '//*[@id="datedtime"]').text)
        return self.__status(price, t[0], last)

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
        if tab.upper() in lf:
            self.__update(tab.upper(), 'open', _)
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
        if tab in lf:
            self.__update(tab, 'high', _)

    def update_low(self, tab, _):
        tab = tab.upper()
        if tab in lf:
            self.__update(tab, 'low', _)

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
        if isinstance(code, (int, float)):
            code = str(int(code))
        self.reset('Local')
        for _ in self.browser.find_elements_by_tag_name('option'):
            if _.text == code:
                _.click()
                break
        self.browser.find_element_by_tag_name('button').click()

    def __load(self, tabs):
        for _ in [__ for __ in tabs if __ in lf]:
            self.browser.execute_script(
                f"window.open('http://{self.lip}','{_}');")
            self.goto(_)
            for __ in self.browser.find_elements_by_tag_name('option'):
                if __.text == _:
                    __.click()
                    break
            self.wait.until(EC.presence_of_element_located(
                (By.TAG_NAME, 'button')))
            t = (_[0] + _[-2] + 'b').lower()
            exec(f"self.{t}=self.browser.find_element_by_tag_name('button')")
            for __ in fields:
                t = (_[0] + _[-2] + __[0]).lower()
                exec(f"self.{t}=self.browser.find_element_by_name('{__}')")


def dow(wf, _=True):
    _wf = copy.copy(wf)
    return _wf.usif('Dow', implied=_)


def nq(wf, _=True):
    _wf = copy.copy(wf)
    return _wf.usif('Nasdaq', implied=_)


def sp(wf, _=True):
    _wf = copy.copy(wf)
    return _wf.usif('S&P', implied=_)


def nk(wf):
    _wf = copy.copy(wf)
    return _wf.nk225()


def sc(wf):
    _wf = copy.copy(wf)
    return _wf.shanghai_composite()


def display(wf, interval=50):
    # import copy
    from time import sleep
    _wf = copy.copy(wf)
    while True:
        try:
            print(f'Time: {datetime.now():%H:%M:%S}')
            _d, _n, _s = dow(_wf), nk(_wf), sc(_wf)
            print(f'Dow:\t{_d[0]}\t{_d[1]}\t{_d[2]}\t{_d[-1]}')
            print(f'Nikkei:\t{_n[0]}\t{_n[1]}\t{_n[2]}\t{_n[-1]}')
            print(f'Sha C.:\t{_s[0]}\t{_s[1]}\t{_s[2]}\t{_s[-1]}\n')
            sleep(interval)
        except Exception:
            pass
