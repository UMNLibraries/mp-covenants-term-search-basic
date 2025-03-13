# mp-covenants-term-search-basic

This AWS Lambda function is part of [Mapping Prejudice's](https://mappingprejudice.umn.edu/) Deed Machine application. This component looks for racial and other terms to flag as potential racial covenants. For each term in covenant_flags, check OCR JSON file received from previous step for existance of term. This is a really simple CNTRL + F style search, no partial matching, regex, fuzzy, etc. Some of the terms are actually exceptions rather than covenant hits, and once they reach the Django stage, will be used to mark this page as exempt from consideration as being considered as a racial covenant. Common examples of exceptions include birth certificates and military discharges, which often contain racial information but are not going to contain a racial covenant. This is the third Lambda in the Deed Machine initial processing Step Function.

The [Deed Machine](https://github.com/UMNLibraries/racial_covenants_processor/) is a multi-language set of tools that use OCR and crowdsourced transcription to identify racially restrictive covenant language, then map the results.

The Lambda components of the Deed Machine are built using Amazon's Serverless Application Model (SAM) and the AWS SAM CLI tool.

## Key links
- [License](https://github.com/UMNLibraries/racial_covenants_processor/blob/main/LICENSE)
- [Component documentation](https://the-deed-machine.readthedocs.io/en/latest/modules/lambdas/mp-covenants-term-search-basic.html)
- [Documentation home](https://the-deed-machine.readthedocs.io/en/latest/)
- [Downloadable Racial covenants data](https://github.com/umnlibraries/mp-us-racial-covenants)
- [Mapping Prejudice main site](https://mappingprejudice.umn.edu/)

## Software development requirements
- Pipenv (Can use other virtual environments, but will require fiddling on your part)
- AWS SAM CLI
- Docker
- Python 3

## Quickstart commands

To build the application:

```bash
pipenv install
pipenv shell
sam build
```

To rebuild and deploy the application:

```bash
sam build && sam deploy
```

Example with non-default aws profile:

```bash
sam build && sam deploy --profile contracosta --config-env contracosta
```

To run tests:

```bash
pytest
```