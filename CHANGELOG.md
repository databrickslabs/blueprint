# Version changelog

## 0.3.0

* Added automated upgrade framework ([#50](https://github.com/databrickslabs/blueprint/issues/50)). This update introduces an automated upgrade framework for managing and applying upgrades to the product, with a new `upgrades.py` file that includes a `ProductInfo` class having methods for version handling, wheel building, and exception handling. The test code organization has been improved, and new test cases, functions, and a directory structure for fixtures and unit tests have been added for the upgrades functionality. The `test_wheels.py` file now checks the version of the Databricks SDK and handles cases where the version marker is missing or does not contain the `__version__` variable. Additionally, a new `Application State Migrations` section has been added to the README, explaining the process of seamless upgrades from version X to version Z through version Y, addressing the need for configuration or database state migrations as the application evolves. Users can apply these upgrades by following an idiomatic usage pattern involving several classes and functions. Furthermore, improvements have been made to the `_trim_leading_whitespace` function in the `commands.py` file of the `databricks.labs.blueprint` module, ensuring accurate and consistent removal of leading whitespace for each line in the command string, leading to better overall functionality and maintainability.
* Added brute-forcing `SerdeError` with `as_dict()` and `from_dict()` ([#58](https://github.com/databrickslabs/blueprint/issues/58)). This commit introduces a brute-forcing approach for handling `SerdeError` using `as_dict()` and `from_dict()` methods in an open-source library. The new `SomePolicy` class demonstrates the usage of these methods for manual serialization and deserialization of custom classes. The `as_dict()` method returns a dictionary representation of the class instance, and the `from_dict()` method, decorated with `@classmethod`, creates a new instance from the provided dictionary. Additionally, the GitHub Actions workflow for acceptance tests has been updated to include the `ready_for_review` event type, ensuring that tests run not only for opened and synchronized pull requests but also when marked as "ready for review." These changes provide developers with more control over the deserialization process and facilitate debugging in cases where default deserialization fails, but should be used judiciously to avoid brittle code.
* Fixed nightly integration tests run as service principals ([#52](https://github.com/databrickslabs/blueprint/issues/52)). In this release, we have enhanced the compatibility of our codebase with service principals, particularly in the context of nightly integration tests. The `Installation` class in the `databricks.labs.blueprint.installation` module has been refactored, deprecating the `current` method and introducing two new methods: `assume_global` and `assume_user_home`. These methods enable users to install and manage `blueprint` as either a global or user-specific installation. Additionally, the `existing` method has been updated to work with the new `Installation` methods. In the test suite, the `test_installation.py` file has been updated to correctly detect global and user-specific installations when running as a service principal. These changes improve the testability and functionality of our software, ensuring seamless operation with service principals during nightly integration tests.
* Made `test_existing_installations_are_detected` more resilient ([#51](https://github.com/databrickslabs/blueprint/issues/51)). In this release, we have added a new test function `test_existing_installations_are_detected` that checks if existing installations are correctly detected and retries the test for up to 15 seconds if they are not. This improves the reliability of the test by making it more resilient to potential intermittent failures. We have also added an import from `databricks.sdk.retries` named `retried` which is used to retry the test function in case of an `AssertionError`. Additionally, the test function `test_existing` has been renamed to `test_existing_installations_are_detected` and the `xfail` marker has been removed. We have also renamed the test function `test_dataclass` to `test_loading_dataclass_from_installation` for better clarity. This change will help ensure that the library is correctly detecting existing installations and improve the overall quality of the codebase.


## 0.2.5

* Automatically enable workspace filesystem if the feature is disabled ([#42](https://github.com/databrickslabs/blueprint/pull/42)).


## 0.2.4

* Added more integration tests for `Installation` ([#39](https://github.com/databrickslabs/blueprint/pull/39)).
* Fixed `yaml` optional import error ([#38](https://github.com/databrickslabs/blueprint/pull/38)).


## 0.2.3

* Added special handling for notebooks in `Installation.upload(...)` ([#36](https://github.com/databrickslabs/blueprint/pull/36)).


## 0.2.2

* Fixed issues with uploading wheels to DBFS and loading a non-existing install state ([#34](https://github.com/databrickslabs/blueprint/pull/34)).


## 0.2.1

* Aligned `Installation` framework with UCX project ([#32](https://github.com/databrickslabs/blueprint/pull/32)).


## 0.2.0

* Added common install state primitives with strong typing ([#27](https://github.com/databrickslabs/blueprint/pull/27)).
* Added documentation for Invoking Databricks Connect ([#28](https://github.com/databrickslabs/blueprint/pull/28)).
* Added more documentation for Databricks CLI command router ([#30](https://github.com/databrickslabs/blueprint/pull/30)).
* Enforced `pylint` standards ([#29](https://github.com/databrickslabs/blueprint/pull/29)).


## 0.1.0

* Changed python requirement from 3.10.6 to 3.10 ([#25](https://github.com/databrickslabs/blueprint/pull/25)).


## 0.0.6

* Make `find_project_root` more deterministic ([#23](https://github.com/databrickslabs/blueprint/pull/23)).


## 0.0.5

* Make it work with `ucx` ([#21](https://github.com/databrickslabs/blueprint/pull/21)).


## 0.0.4

* Fixed sigstore action ([#19](https://github.com/databrickslabs/blueprint/pull/19)).


## 0.0.3

* Sign artifacts with Sigstore ([#17](https://github.com/databrickslabs/blueprint/pull/17)).


## 0.0.2

* Added extensive library documentation ([#14](https://github.com/databrickslabs/blueprint/pull/14)).
* Setup release to PyPI via GitHub OIDC ([#15](https://github.com/databrickslabs/blueprint/pull/15)).


## Release 0.0.1

* Added `.codegen.json` and `CHANGELOG.md` templates for automated releases.
* Added `CODEOWNERS` for code governance.
* Added command framework for Databricks CLI launcher frontend ([#10](https://github.com/databrickslabs/blueprint/pull/10)).
* Added ProductInfo unreleased version fallback ([#9](https://github.com/databrickslabs/blueprint/pull/9)).
