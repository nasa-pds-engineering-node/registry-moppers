'''change single strings to array of strings'''

def repair (document:{}, fieldname:str)->bool:
    if isinstance (document[fieldname], str):
        log.info (f'found string for {fieldname} where it should be an array')
        return {fieldname:[document[fieldname]}
    return {}
