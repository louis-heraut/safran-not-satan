# build package documentation
doc:
	R -e 'devtools::document()'

check:
	R -e 'devtools::check()'

install:
	R -e 'remotes::install_github("louis-heraut/safran-not-satan")'

github_check:
	R -e 'usethis::use_github_action_check_standard()'
