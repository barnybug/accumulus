#!/usr/bin/env python

import urllib2
import json
import boto.ec2
import re
import os
import yaml
from itertools import groupby
from jsonpath import jsonpath
from operator import attrgetter as ag
import logging
import logging.config

def invert_dict(d):
    return dict((v, k) for k, v in d.iteritems())

CONSTANTS = yaml.load(file('constants.yml'))

AZ_REGIONS = CONSTANTS['regions']
REGIONS_AZ = invert_dict(AZ_REGIONS)

AZ_TYPES = CONSTANTS['types']
TYPES_AZ = invert_dict(AZ_TYPES)

AZ_SUBTYPES = CONSTANTS['subtypes']
SUBTYPES_AZ = invert_dict(AZ_SUBTYPES)

URLS = CONSTANTS['urls']

SETTINGS = yaml.load(file('settings.yml'))

class classproperty(object):
    def __init__(self, f):
        self.f = f
    
    def __get__(self, obj, owner):
        return self.f(owner)

class Cache(object):
    def __init__(self):
        self.logger = logging.getLogger('cache')
        self.path = os.path.abspath('cache')
        if not os.path.exists(self.path):
            os.makedirs(self.path)
    
    def _key(self, url):
        return re.sub(r'\W', '_', url)
    
    def get(self, url):
        key = os.path.join(self.path, self._key(url))
        if os.path.exists(key):
            self.logger.info('Using cache for %r' % url)
            with file(key, 'r') as fin:
                data = fin.read()
        else:
            self.logger.info('Caching %r' % url)
            req = urllib2.urlopen(url)
            data = req.read()
            with file(key, 'w') as fout:
                fout.write(data)
        return data

    @classproperty
    def instance(cls):
        cls.instance = Cache()
        return cls.instance

class PricingModel(object):
    def load(self):
        self.ondemand = json.loads(Cache.instance.get(URLS['ondemand']))
        self.heavylinux = json.loads(Cache.instance.get(URLS['heavylinux']))
        self.heavywindows = json.loads(Cache.instance.get(URLS['heavywin']))

    def price(self, region, ins, arch, reserved):
        if reserved:
            return self.reserved_price(region, ins, arch, reserved)
        else:
            return self.ondemand_price(region, ins, arch)
        
    def ondemand_price(self, region, ins, arch):
        i1, i2 = ins.split('.')
        if arch == 'windows':
            arch = 'mswin'
        jpath = 'config.regions[?(@.region==%r)].instanceTypes[?(@.type==%r)].sizes[?(@.size==%r)].valueColumns[?(@.name==%r)].prices.USD' % (
            REGIONS_AZ[region],
            TYPES_AZ[i1],
            SUBTYPES_AZ[i2],
            arch,
        )
        r = jsonpath(self.ondemand, jpath)
        if not r:
            logging.error('Price not found for: %r %r %r' % (region, ins, arch))
            return 0.0
        return float(r[0])

    def reserved_price(self, region, ins, arch, reserved):
        region = str(region)
        if region == 'us-east-1':
            region = 'us-east' # naming inconsistency!
        i1, i2 = ins.split('.')
        r1 = TYPES_AZ[i1].replace('OD', 'Res')
        assert arch in ('linux', 'windows')
        if arch == 'linux':
            data = self.heavylinux
        else:
            data = self.heavywindows
        jpath = 'config.regions[?(@.region==%r)].instanceTypes[?(@.type==%r)].sizes[?(@.size==%r)]' % (
            region,
            r1,
            SUBTYPES_AZ[i2],
        )
        jpath1 = jpath+'.valueColumns[?(@.name==%r)].prices.USD' % 'yrTerm%dHourly' % reserved
        jpath2 = jpath+'.valueColumns[?(@.name==%r)].prices.USD' % 'yrTerm%d' % reserved
        hourly = jsonpath(data, jpath1)
        upfront = jsonpath(data, jpath2)
        if not hourly or not upfront:
            logging.error('Price not found for: %r %r %r' % (region, ins, arch))
            return 0.0
        return float(hourly[0]) + float(upfront[0])/YEARHOURS/reserved

class InstResource(object):
    def __init__(self, account, inst):
        self.account = account
        self.inst = inst
        
    def cost(self, pm, reserved=False):
        arch = self.inst.platform or 'linux'
        return pm.price(self.inst.region.name, self.inst.instance_type, arch, reserved=reserved)
    
    @property
    def instance_type(self):
        return self.inst.instance_type

    @property
    def region(self):
        return self.inst.region.name
    
class Scanner(object):
    def __init__(self, account, config):
        self.logger = logging.getLogger('scanner')
        self.account = account
        self.access_key = config['access_key_id']
        self.secret_key = config['secret_access_key']
        if 'regions' in config:
            self.regions = config['regions']
        else:
            self.regions = AZ_REGIONS.keys()
        
    def scan(self):
        resources = []
        for r in self.regions:
            self.logger.info('Scanning %s / %s' % (self.account, r))
            ec2 = boto.ec2.connect_to_region(r,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key)

            # get reservations
            rs = ec2.get_all_reserved_instances(
                filters={'state': 'active'})

            reservations = []
            for r in rs:
                for x in xrange(r.instance_count):
                    p = (r.instance_type, r.availability_zone, r.duration / 31536000)
                    self.logger.info('Reservation: %s / %s / %s' % p)
                    reservations.append(p)

            # get running instances
            resources.extend(self._instances(ec2, reservations))

            for unused in reservations:
                self.logger.warn('Unused reservation: %s / %s / %s' % unused)

        return resources

    def _instances(self, ec2, reservations):
        filters = {'instance-state-name': 'running'}
        insts = list( i for r in ec2.get_all_instances(filters=filters) for i in r.instances )
        for inst in insts:
            if inst.spot_instance_request_id:
                # ignore spot instances
                continue
            resource = InstResource(self.account, inst)
            self.logger.info('Instance: %s / %s / %s'
                % (inst.id, inst.instance_type, inst.placement))

            r1 = (inst.instance_type, inst.placement, 1)
            r3 = (inst.instance_type, inst.placement, 3)
            if r1 in reservations:
                reservations.pop(reservations.index(r1))
                resource.reserved = 1
                self.logger.info('Using reservation: %s / %s / %s' % r1)
            elif r3 in reservations:
                reservations.pop(reservations.index(r3))
                resource.reserved = 3
                self.logger.info('Using reservation: %s / %s / %s' % r3)
            else:
                resource.reserved = False
            yield resource

# average number of hours in a month
YEARHOURS = 365 * 24.0
MONTHHOURS = YEARHOURS / 12.0

class Calculator():
    def __init__(self, pm):
        self.logger = logging.getLogger('main')
        self.pm = pm
        
    def render(self):
        resources = []
        for account, config in SETTINGS['accounts'].iteritems():
            scanner = Scanner(account, config)
            resources.extend(scanner.scan())

        import html

        doc = html.HTML()
        html = doc.html
        head = html.head

        head.title('Amazon cloudcash statement')
        # stylesheet is embedded for email
        head.style(file('style.css').read())

        body = html.body
        table = body.table

        tr = table.tr
        tr.th('')
        # tr.td('total', colspan=2)
        tr.th('on-demand', klass="top", colspan='2')
        tr.th('reserved', klass="top", colspan='2')
        tr.th('total', klass="top")
        tr.th('savings possible<br/>(1yr reservations)', escape=False, colspan='2', klass="top")
        tr.th('savings possible<br/>(3yr reservations)', escape=False, colspan='2', klass="top")

        tr = table.tr
        tr.th('')
        tr.th('#', klass='num unit')
        tr.th('$/month', klass='unit')
        tr.th('#', klass='num unit')
        tr.th('$/month', klass='unit')
        tr.th('$/month', klass='unit')
        tr.th('$/month', klass='unit')
        tr.th('%', klass='unit')
        tr.th('$/month', klass='unit')
        tr.th('%', klass='unit')

        def row(label, res, level):
            tr = table.tr(klass="level%d" % level)
            tr.td(label, klass="label")
            od = sum(1 for r in res if not r.reserved)
            odcost = sum(r.cost(self.pm, reserved=False) for r in res if not r.reserved) * MONTHHOURS
            odat1yrcost = sum(r.cost(self.pm, reserved=1) for r in res if not r.reserved) * MONTHHOURS
            odat3yrcost = sum(r.cost(self.pm, reserved=3) for r in res if not r.reserved) * MONTHHOURS
            rv = sum(1 for r in res if r.reserved)
            rvcost = sum(r.cost(self.pm, reserved=r.reserved) for r in res if r.reserved) * MONTHHOURS

            def i(n, c):
                if n > 0:
                    tr.td(str(n), klass="num")
                    tr.td('%.2f' % c, klass="cost")
                else:
                    tr.td('-', colspan='2', align='center')

            i(od, odcost)
            i(rv, rvcost)

            # total
            total = odcost + rvcost
            tr.td('%.2f' % total, klass="cost")
            # savings possible
            savings = odcost - odat1yrcost
            tr.td('%.2f' % savings, klass="cost")
            tr.td('%d%%' % (savings/total*100), klass="cost")
            savings = odcost - odat3yrcost
            tr.td('%.2f' % savings, klass="cost")
            tr.td('%d%%' % (savings/total*100), klass="cost")


        res = sorted(resources,
            key=lambda i: (i.account, i.region, i.instance_type))

        row('Total', res, 0)
        for account, g in groupby(res, ag('account')):
            res = list(g)
            row(account, res, 1)
            for region, g in groupby(res, ag('region')):
                res = list(g)
                row(region, res, 2)
                for it, g in groupby(res, ag('instance_type')):
                    res = list(g)
                    row(it, res, 3)

        doc.p('note: all prices exclude Taxes and VAT.')

        filename = 'bill.html'
        with file(filename, 'w') as fout:
            print >>fout, doc
        self.logger.info('Generated %s' % filename)
        
def main():
    logging.config.fileConfig('logging.conf')

    pm = PricingModel()
    pm.load()
    cal = Calculator(pm)
    cal.render()

main()
