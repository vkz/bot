;;; Directory Local Variables
;;; For more information see (info "(emacs) Directory Variables")

((clojure-mode
  ;; (cider-clojure-cli-global-options . "-J'--add-modules=java.xml.bind'")
  (cider-clojure-cli-global-options . "-O:xmlbind -R:lispy")
  ;; (cider-lein-global-options . "--add-modules=java.xml.bind")
  ;; tests are in the same namespace
  (cider-test-infer-test-ns . (lambda (ns) ns))))
