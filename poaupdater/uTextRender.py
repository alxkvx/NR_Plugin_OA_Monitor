# -*- coding: utf-8 -*-


class Align:
    LEFT = "left"
    RIGHT = "right"
    CENTER = "center"


class Table:

    _available_alignments = [Align.LEFT, Align.RIGHT, Align.CENTER]

    def __init__(self, indent=1, decoration=('=', '-', '|')):
        self.headers = []
        self.columnAlignments = []
        self.rows = []
        self.text = ""
        self.indent = indent
        self.headerDelimiterSymbol, self.interlineDelimiterSymbol, self.columnDelimiter = decoration
        self.rendered = False
        self.width = 0  # total width of table
        self.columnWidths = {}  # map containing width of each column with col's index as key

    def setIndent(self, indent):
        """
        Sets width of left indent of table.
        Has no effect after table is rendered.
        :param indent: integer - width of left indent
        """
        self.indent = indent

    def setHeader(self, captions):
        """
        Sets header captions of the table.
        The number of captions is the number of columns in the table.
        Setting headers discards rows of table.
        :param captions: array of strings without line separators.
        """
        self.rows = []
        self.rendered = False
        self.headers = [caption.replace('\n', ' ') for caption in captions]
        self.columnAlignments = ([Align.LEFT] * self._get_col_count())
        self.width = self._get_col_count() * 3 + 1
        for col_id in range(self._get_col_count()):
            self.columnWidths[col_id] = len(self.headers[col_id])
            self.width += self.columnWidths[col_id]

    def setColumnAlignment(self, alignments):
        """
        Sets column alignments.
        :param alignments: an array of one of the following Align.LEFT, Align.RIGHT, Align.CENTER.
        """
        self.columnAlignments = self._adjust_data(alignments, Align.LEFT)

    def addRow(self, row):
        """
        Adds a new row to table and adjust metrics of columns and table.
        :param row: an array of strings.
        """
        new_row = self._adjust_data(row)
        for col_id in range(self._get_col_count()):
            cell = new_row[col_id]
            if '\n' in cell:
                for line in cell.split('\n'):
                    l = len(line)
                    if l > self.columnWidths[col_id]:
                        self.width -= self.columnWidths[col_id]
                        self.columnWidths[col_id] = l
                        self.width += self.columnWidths[col_id]
            else:
                l = len(cell)
                if l > self.columnWidths[col_id]:
                    self.width -= self.columnWidths[col_id]
                    self.columnWidths[col_id] = l
                    self.width += self.columnWidths[col_id]
        self.rows += [new_row]

    def _get_col_count(self):
        return len(self.headers)

    def __unicode__(self):
        if not self.rendered:
            self._render_data()
        return self.text

    def __str__(self):
        return unicode(self).encode('utf-8')

    def _render_data(self):

        self.text += (self._get_indent_string() + (self.headerDelimiterSymbol * self.width))

        self.text += "\n" + self._get_indent_string() + self.columnDelimiter
        for col_id in range(self._get_col_count()):
            header = self.headers[col_id]
            self.text += (self._render_cell(header, Align.CENTER, self.columnWidths[col_id]) + self.columnDelimiter)
        self.text += ("\n" + self._get_indent_string() + (self.headerDelimiterSymbol * self.width) + "\n")

        for row in self.rows:
            self.text += self._render_row(row)
            self.text += self._get_interline_delimiter()
            self.text += "\n"

        self.rendered = True

    def _render_row(self, row):
        max_lines = 1
        for cell in row:
            if '\n' in cell:
                if cell.endswith('\n'):
                    cell = cell[:-1]
                cell_lines = cell.split('\n')
                ll = len(cell_lines)
                if ll > max_lines:
                    max_lines = ll

        out_text = ""
        for li in range(max_lines):
            out_text += (self._get_indent_string() + self.columnDelimiter)
            for col_id in range(self._get_col_count()):
                cell = row[col_id]
                cell_lines = cell.split('\n')
                ll = len(cell_lines)
                cell_lines = cell_lines + ([""] * (max_lines - ll))
                out_text += (self._render_cell(cell_lines[li], self.columnAlignments[col_id],
                                               self.columnWidths[col_id]) + self.columnDelimiter)
            out_text += "\n"
        return out_text

    def _adjust_data(self, array, default=""):
        cols = self._get_col_count()
        data_cols = len(array)
        if data_cols > cols:
            array = [item.strip() for item in array[:cols]]
        elif data_cols < cols:
            array = [item.strip() for item in array + ([default] * (cols - data_cols))]
        else:
            array = [item.strip() for item in array]
        return array

    def _render_cell(self, text, alignment, width):
        l = len(text)
        if alignment == Align.CENTER:
            rd = (width - l) / 2
            ld = width - l - rd
            return " %s%s%s " % ((" " * ld), text, (" " * rd))
        elif alignment == Align.RIGHT:
            return " %s%s " % ((" " * (width - l)), text)
        else:
            return " %s%s " % (text, (" " * (width - l)))

    def _get_indent_string(self):
        return " " * int(self.indent)

    def _get_interline_delimiter(self):
        return self._get_indent_string() + (self.interlineDelimiterSymbol * self.width)
