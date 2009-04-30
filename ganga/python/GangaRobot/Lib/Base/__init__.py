"""Abstract submit / finish / extract / report action implementations.

See BaseSubmitter, BaseFinisher, BaseExtractor and BaseReporter for abstract
implementations providing the basis of submit, finish, extract and report
actions using simple data models for the extracted data and the reports,
Extract.Node and Report.Report respectively.

Concrete implementations should extend the base actions by implementing the
handleXXX methods, which pass Extract.Node or Report.Report objects where
appropriate.

See GangaRobot.Lib.Core for concrete implementations which extend these base
implementations.

"""