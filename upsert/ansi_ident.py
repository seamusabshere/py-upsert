import codecs
import upsert

class AnsiIdent:
    # http://stackoverflow.com/questions/6514274/how-do-you-escape-strings-for-sqlite-table-column-names-in-python
    @upsert.memoize
    def quote_ident(self, str):
        encodable = str.encode("utf-8", "strict").decode("utf-8")
        nul_index = encodable.find("\x00")
        if nul_index >= 0:
            error = UnicodeEncodeError("NUL-terminated utf-8", encodable, nul_index, nul_index + 1, "NUL not allowed")
            error_handler = codecs.lookup_error(errors)
            replacement, _ = error_handler(error)
            encodable = encodable.replace("\x00", replacement)
        return '"' + encodable.replace('"', '""') + '"'
