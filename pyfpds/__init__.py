# -*- coding: utf-8 -*-
from __future__ import print_function #needs to be at the top

from collections import OrderedDict
from datetime import datetime
from time import sleep

import xmltodict
import requests
import json
import warnings


__author__ = 'Kaitlin Devine'
__email__ = 'katycorp@gmail.com'
__version__ = '0.1.0'


warnings.filterwarnings('ignore')


field_map = {    
    'piid': 'PIID',
    'idv_piid': 'REF_IDV_PIID',
    'idv_agency_id': 'REF_IDV_AGENCY_ID',
    'modification_number': 'MODIFICATION_NUMBER',
    
    'contracting_agency_id': 'CONTRACTING_AGENCY_ID',
    'contracting_agency_name': 'CONTRACTING_AGENCY_NAME', 
    'contracting_office_id': 'CONTRACTING_OFFICE_ID',
    'contracting_office_name': 'CONTRACTING_OFFICE_NAME',
    'funding_agency_id': 'FUNDING_AGENCY_ID',
    'funding_office_id': 'FUNDING_OFFICE_ID',
    'funding_office_name': 'FUNDING_OFFICE_NAME',
    'agency_code': 'AGENCY_CODE',
    'agency_name': 'AGENCY_NAME',
    'department_id': 'DEPARTMENT_ID',
    'department_name': 'DEPARTMENT_NAME',

    'last_modified_date': 'LAST_MOD_DATE',
    'last_modified_by': 'LAST_MODIFIED_BY',
    'award_completion_date': 'AWARD_COMPLETION_DATE',
    'created_on': 'CREATED_DATE',
    'date_signed': 'SIGNED_DATE',
    'effective_date': 'EFFECTIVE_DATE',
    'estimated_completion_date': 'ESTIMATED_COMPLETION_DATE',
    
    'obligated_amount': 'OBLIGATED_AMOUNT',
    'ultimate_contract_value': 'ULTIMATE_CONTRACT_VALUE',
    'contract_pricing_type': 'TYPE_OF_CONTRACT_PRICING',
    
    'award_status': 'AWARD_STATUS',
    'contract_type': 'CONTRACT_TYPE',
    'created_by': 'CREATED_BY',
    'description': 'DESCRIPTION_OF_REQUIREMENT',
    'modification_reason': 'REASON_FOR_MODIFICATION',
    'legislative_mandates': 'LEGISLATIVE_MANDATES',
    'local_area_setaside': 'LOCAL_AREA_SET_ASIDE',
    'socioeconomic_indicators': 'SOCIO_ECONOMIC_INDICATORS',
    'multiyear_contract': 'MULTIYEAR_CONTRACT',
    'national_interest_code': 'NATIONAL_INTEREST_CODE',
    'national_interest_description': 'NATIONAL_INTEREST_DESCRIPTION',
    
    'naics_code': 'PRINCIPAL_NAICS_CODE',
    'naics_description': 'NAICS_DESCRIPTION',
    'product_or_service_code': 'PRODUCT_OR_SERVICE_CODE',
    'product_or_service_description': 'PRODUCT_OR_SERVICE_DESCRIPTION',
    
    'place_of_performance_district': 'POP_CONGRESS_DISTRICT_CODE',
    'place_of_performance_country': 'POP_CONGRESS_COUNTRY',
    'place_of_performance_state': 'POP_STATE_NAME',

    'vendor_city': 'VENDOR_ADDRESS_CITY',
    'vendor_district': 'VENDOR_CONGRESS_DISTRICT_CODE',
    'vendor_country_code': 'VENDOR_ADDRESS_COUNTRY_CODE',
    'vendor_country_name': 'VENDOR_ADDRESS_COUNTRY_NAME',
    'vendor_duns': 'VENDOR_DUNS_NUMBER',
    'vendor_dba_name': 'VENDOR_DOING_BUSINESS_AS_NAME',
    'vendor_name': 'VENDOR_NAME',
    'vendor_state_code': 'VENDOR_ADDRESS_STATE_CODE',
    'vendor_state_name': 'VENDOR_ADDRESS_STATE_NAME',
    'vendor_zip': 'VENDOR_ADDRESS_ZIP_CODE',
}


boolean_map = {
    True: 'Y',
    False: 'N',
}


class Contracts():
    
    feed_url = "https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&q="
    
        
    def __init__(self, logger=None):
        #point logger to a log function, print by default
        if logger:
            self.log = logger 
        else:
            self.log = print


    def pretty_print(self, data):
        self.log(json.dumps(data, indent=4))

    
    def date_format(self, date1, date2):
        return "[{0},{1}]".format(date1.strftime("%Y/%m/%d"), date2.strftime("%Y/%m/%d"))


    def convert_params(self, params):
        new_params = {}
        for k,v in params.items():
            new_params[field_map[k]] = v
        return new_params


    def combine_params(self, params):
        return " ".join("%s:%s" % (k,v) for k,v in params.items())


    def process_data(self, data):
        #todo
        if isinstance(data, dict):
            #make a list so it's consistent
            data = [data,]
        return data

    
    def get_last_modified_date(self, entry):
        try:
            if 'IDV' in entry:
                award = entry['IDV']
            else:
                award = entry['award']
         
            transaction = award['transactionInformation']
        
            if 'lastModifiedDate' in transaction:
                date_string = transaction['lastModifiedDate']
            else:
                date_string = transaction['createdDate']
            
            return datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        
        except Exception as e:
            return None
    
    
    def get_api_results(self, params, start_index=1):
        self.log("querying {0}{1}&start={2}".format(self.feed_url, params, start_index))        
        
        resp = requests.get(self.feed_url + params + '&start={0}'.format(start_index), timeout=60, verify = False)
        resp_data = xmltodict.parse(resp.text, process_namespaces=True, namespaces={'http://www.fpdsng.com/FPDS': None, 'http://www.w3.org/2005/Atom': None})        
        
        self.log("finished querying {0}".format(resp.url))
        try:
            processed_data = self.process_data(resp_data['feed']['entry'])

        except KeyError as e:
            return None
        
        return processed_data


    def get(self, num_records=100, order='desc', **kwargs):
        data = []
        i = 0
        
        #For some reason FPDS-NG is returning last modified records outside of requested range
        #which can blow up the system (memory usage issues), so check for proper modified timestamp
        #or none in the FPDS data before adding to final product.  This should free up some space.
        if 'last_modified_date' in kwargs and isinstance(kwargs['last_modified_date'], list):
            first_date = kwargs['last_modified_date'][0]
            last_date = kwargs['last_modified_date'][1]
            kwargs['last_modified_date'] = self.date_format(first_date, last_date)
        else:
            first_date = None
            last_date = None
        
        params = self.combine_params(self.convert_params(kwargs))
        
        while num_records == "all" or i < num_records:
            if 'sleep' in kwargs and int(kwargs['sleep']) > 0:
                sleep(int(kwargs['sleep']))
            
            processed_data = self.get_api_results(params, i)
             
            if processed_data is not None:
                for pd in processed_data:
                    i += 1
                    
                    pd['modified'] = self.get_last_modified_date(pd['content'])
                     
                    #This code is the makeshift attempt to correct for a possible bug in the
                    #FPDS-NG ATOM feed api date range selector that returns results outside
                    #the last updated range requested (often by many years)
                    #
                    #This intentionally has no effect if string modification date format is used
                    #
                    if pd['modified']:
                        if first_date and pd['modified'].date() < first_date:
                            continue
                        if last_date and pd['modified'].date() > last_date:
                            continue
                    
                    data.append(pd)
                
                #if data contains less than 10 records, break out of loop
                if len(processed_data) < 10:
                    break
            
            else:
                #no results
                self.log("No results for query")
                break
        
        return data
