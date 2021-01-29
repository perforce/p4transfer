# Makefile for SDP docs from AsciiDoctor format to HTML and PDF

SRC = $(wildcard *.adoc)

PDF = $(SRC:.adoc=.pdf)

HTML = $(SRC:.adoc=.html)

# Default target 'all' builds PDF and HTML from *.adoc files. For
# any referenced generated man pages, current files in the 'gen'
# directory are used.  See 'full'.
all:	$(PDF) $(HTML)

# General Rule for converting AsciiDoc files to HTML.
%.html: %.adoc
	asciidoctor $^

# General Rule for converting AsciiDoc files to PDF.
%.pdf: %.adoc
	asciidoctor-pdf -a pdf-themesdir=themes -a pdf-theme=basic $^


.PHONY: clean

clean:
	rm -f $(PDF) $(HTML)
