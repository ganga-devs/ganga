class ConfigSection:
    name=''
    attributes={}
    
    def __init__(self,name):
        self.name = name
        self.attributes={}
        
    def addattribute(self, p , val):
        self.attributes[p] = val
        
    def write(self,filename):
        tag = '['+self.name+']\n'
        filename.write(tag)
        filename.write('\n')
        for k, v in self.attributes.items():
            p = '%s = %s\n' % (k, v)
            filename.write(p)
        filename.write('\n')
    
