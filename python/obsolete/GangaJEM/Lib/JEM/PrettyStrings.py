import os, time


def makeHeader(title):
    return title + "\n" + ("-" * 120) + "\n"


def formatString(value, maxlen):
    if len(value) <= maxlen:
        return value.ljust(maxlen)
    else:
        return value[:(maxlen-3)] + "..."

 
def formatDatum(name, value):
    return name.ljust(35) + ": " + value + "\n"


def formatTime(data):
    try:
        if data.has_key("ExactTS") and data["ExactTS"] != "???" and data["ExactTS"] != "\"???\"":
            if data["ExactTS"].find(".") != -1:
                s,ms = data["ExactTS"].split(".")
            else:
                s = data["ExactTS"]
                ms = "000000"
            while len(ms) < 6:
                ms = "0" + ms
            return time.strftime("%H:%M:%S",time.localtime(float(s))) + "." + ms[:3]
        elif data.has_key("TS"):
            return data["TS"].split(" ")[1]
        else:
            return "<unknown>"
    except:
        return "<unknown>"


def formatFileName(filename):
    if filename.find(os.sep) != -1:
        filename = filename.split(os.sep)[-1]
    if len(filename) > 24:
        filename = "..." + filename[-22:]
    return filename


def formatLocation(data):
    s = ""

    if data.has_key("Frame"):
        s += formatString(data["Frame"], 32)
    else:
        s += "???".ljust(32)

    s += " ("

    if data.has_key("Script"):
        s += formatFileName(data["Script"])
    else:
        s += "???"

    s += ":"

    if data.has_key("Line"):
        s += data["Line"]
    else:
        s += "?"

    s += ")"
    
    return s


def formatVarList(title, ss):
    ret = ""
    if ss != None:
        try:
            value = {}
            exec("value = " + ss) # pylint: disable-msg=W0122
                                  #   - yes, I *know* what I'm doing!
            if len(value) > 0:
                ret += "\n" + title + "\n"
                for k,v in value.iteritems():
                    if isinstance(k, str) and isinstance(v, tuple):
                        try:
                            ret += formatString(k.strip(), 24) + " : "
                            ret += formatString(v[1].strip(), 48)
                            if v[0].strip() != '':
                                ret += " [" + formatString(v[0].strip(), 32) + "]"
                        except:
                            pass
                    elif isinstance(k, str) and isinstance(v, str):
                        ret += formatString(k.strip(), 24) + " : "
                        ret += formatString(v.strip(), 48)
                    else:
                        continue
                    ret += "\n"
        except:
            pass
    return ret
