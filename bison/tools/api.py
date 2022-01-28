"""Module to query APIs and return data."""
NEWLINE = "\n"
CR_RETURN = "\r"


# .............................................................................
class APISvc(object):
    """Pulls data from API data services."""

    # ...............................................
    def __init__(self):
        """Construct API service."""
        pass

    # ...............................................
    def _saveNL_delCR(self, strval):
        fval = strval.replace(NEWLINE, "\\n").replace(CR_RETURN, "")
        return fval

    # ...............................................
    def _parse_date(self, datestr):
        datevals = []
        dateonly = datestr.split("T")[0]
        if dateonly != "":
            parts = dateonly.split("-")
            try:
                for i in range(len(parts)):
                    datevals.append(int(parts[i]))
            except ValueError:
                print("Invalid date {}".format(datestr))
                pass
            else:
                if len(datevals) not in (1, 3):
                    print("Non one or three part date {}".format(datevals))
        return datevals

    # ...............................................
    def _first_newer(self, datevals1, datevals2):
        for i in (0, 1, 2):
            # only compare matching year/mo/day
            if len(datevals1) > i and len(datevals2) > i:
                if datevals1[i] > datevals2[i]:
                    return True
                elif datevals1[i] < datevals2[i]:
                    return False
        # if equal, return first_newer
        return True

    # ...............................................
    def _get_data_from_url(self, url, resp_type="json"):
        # implemented in each subclass
        raise NotImplementedError

    # ...............................................
    def _process_record(self, rec, header, reformat_keys=None):
        row = []
        if rec is not None:
            for key in header:
                try:
                    val = rec[key]

                    if type(val) is list:
                        if len(val) > 0:
                            val = val[0]
                        else:
                            val = ""

                    if reformat_keys and key in reformat_keys:
                        val = self._saveNL_delCR(val)

                    elif key == "citation":
                        if type(val) is dict:
                            try:
                                val = val["text"]
                            except KeyError:
                                pass

                    elif key in ("created", "modified"):
                        val = self._parse_date(val)

                except KeyError:
                    val = ""
                row.append(val)
        return row

    # ...............................................
    def _dig_for_val(self, data_dict, keylist, save_nl=True):
        val = ""
        child = data_dict
        for key in keylist:
            try:
                child = child[key]
            except KeyError:
                pass
                # print("Key {} of {} missing from output".format(key, keylist))
            else:
                val = child
        if val != "" and save_nl:
            val = self._saveNL_delCR(val)
        return val

    # ...............................................
    def _get_val(self, rec, key):
        try:
            val = rec[key]
        except KeyError:
            val = None
        return val

    # ...............................................
    def _get_val_or_first_of_list(self, data, key):
        val = ""
        if key in data:
            val = data[key]
            if isinstance(val, list) or isinstance(val, tuple):
                if len(val) > 0:
                    val = val[0]
                else:
                    val = ""
        return val


# ...............................................
if __name__ == "__main__":
    pass
