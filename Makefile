.POSIX:
CLJFLAGS = -J'--add-modules=java.xml.bind'

all: run
run:
	clj -O:xmlbind -m bot
