(formula
 (description "Maquette - A CLOS based ORM library")
 (version "master")
 (homepage :url "https://github.com/ktakashi/sagittarius-maquette")
 (author :name "Takashi Kato" :email "ktakashi@ymail.com")
 (source 
  :type tar :compression gzip
  :url "https://github.com/ktakashi/sagittarius-maquette/archive/master.tar.gz")
 (install (directories ("src")))
 (tests (test :file "tests/tables.scm" :style srfi-64 :loadpath "src")
	(test :file "tests/query.scm" :style srfi-64 :loadpath "src")
	(test :file "tests/connection.scm" :style srfi-64 :loadpath "src")
	(test :file "tests/maquette.scm" :style srfi-64 :loadpath "src")))
