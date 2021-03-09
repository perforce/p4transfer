# Makefile for SDP docs from AsciiDoctor format to HTML and PDF

SRC = $(wildcard *.adoc)

PDF = $(SRC:.adoc=.pdf)

HTML = $(SRC:.adoc=.html)

INCLUDES = help.txt transfer.yaml

# Default target 'all' builds PDF and HTML from *.adoc files. For
# any referenced generated man pages, current files in the 'gen'
# directory are used.  See 'full'.
all:	$(PDF) $(HTML) $(INCLUDES)

# General Rule for converting AsciiDoc files to HTML.
%.html: %.adoc $(INCLUDES)
	asciidoctor $<

# General Rule for converting AsciiDoc files to PDF.
%.pdf: %.adoc $(INCLUDES)
	asciidoctor-pdf -a pdf-themesdir=themes -a pdf-theme=basic $<

help.txt: ../P4Transfer.py
	python3 ../P4Transfer.py -h > $@

transfer.yaml: ../P4Transfer.py
	python3 ../P4Transfer.py --sample-config > $@


.PHONY: clean

clean:
	rm -f $(PDF) $(HTML)
