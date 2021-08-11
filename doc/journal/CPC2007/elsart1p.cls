%%
%% This is file `elsart1p.cls',
%% generated with the docstrip utility.
%%
%% The original source files were:
%%
%% esl.dtx  (with options: `package,elsauth,wrapper,SL1P')
%% 
%% elsart1p.cls Copyright (C) 1994-2006 Elsevier Science
%% 
%% This file may be distributed and/or modified under the
%% conditions of the LaTeX Project Public License, either version 1.2
%% of this license or (at your option) any later version.
%% The latest version of this license is in
%%    http://www.latex-project.org/lppl.txt
%% and version 1.2 or later is part of all distributions of LaTeX
%% version 1999/12/01 or later.
%% 
\def\readRCS$#1: #2 #3 #4 #5${%
 \def\RCSfile{#2}%
 \def\RCSversion{#3}%
 \def\RCSdate{#4}%
}
 \def\esp@fileversion{2.19}
 \def\esp@filedate{2006/05/01}
 \def\@journal{Elsevier} \def\@company{Elsevier}
 \def\@issn{000-0000}
 \def\@shortjid{elsart1p}
\NeedsTeXFormat{LaTeX2e}[1995/12/01]
\ProvidesClass{\@shortjid}[\esp@filedate, \esp@fileversion: \@journal]
\let\@save@journal\@journal
\let\@save@company\@company
\let\@save@shortjid\@shortjid

\DeclareOption{narrowdisplay}{}
\DeclareOption*{\PassOptionsToClass{\CurrentOption}{elsart}}
\ProcessOptions\relax
 \LoadClass[onecolumn]{elsart}

\let\@journal\@save@journal
\let\@company\@save@company
\let\@shortjid\@save@shortjid

\def\@xiiipt{12}
\def\@titlesize{\Large}
\def\@bibitemsep{\z@ \@plus 0.5\p@ \@minus 0.5\p@}
\def\captionwidth{.8\linewidth}
\def\@abstractsize{\fontsize{\@ixpt}{11pt}\selectfont}
\def\@articletypesize{\fontsize{\@xiiipt}{13pt}\selectfont}
\def\normalsize{\@setfontsize\normalsize\@xpt{12}%
\abovedisplayskip 5.75\p@ \@plus 2\p@ \@minus 2\p@%
\belowdisplayskip \abovedisplayskip
\abovedisplayshortskip \z@ \@plus 2\p@%
\belowdisplayshortskip 3.5\p@ \@plus 2\p@ \@minus 2\p@
\let\@listi\@listI}
\def\small{\@setfontsize\small\@viiipt{10}%
\abovedisplayskip 7\p@ \@plus 2\p@ \@minus 4\p@%
\belowdisplayskip \abovedisplayskip
\abovedisplayshortskip \z@ \@plus 1\p@%
\belowdisplayshortskip 3\p@ \@plus 1\p@ \@minus 2\p@
\def\@listi{\topsep 0.5\@bls \parsep\z@ \itemsep\parsep}}
\let\footnotesize=\small
\let\@xviiipt\@xviipt
\def\scriptsize{\@setfontsize\scriptsize\@viipt{8}}
\def\tiny{\@setfontsize\tiny\@vipt{7}}
\def\large{\@setfontsize\large\@xiiipt{14}}
\def\Large{\@setfontsize\Large\@xviipt{20}}
\def\LARGE{\@setfontsize\LARGE\@xviipt{19}}
\def\huge{\@setfontsize\huge\@xxpt{22}}
\def\Huge{\@setfontsize\Huge\@xxvpt{27}}
\normalsize                                % Choose the normalsize font.
\if@twoside
   \oddsidemargin    \z@
   \evensidemargin   \z@
   \marginparwidth  10\p@
\else
   \oddsidemargin    \z@
   \evensidemargin   \z@
   \marginparwidth   2pc
\fi
\marginparsep 20\p@          % Horizontal space between outer margin and
                            % marginal note
\topmargin \z@           %    Nominal distance from top of page to top of
                         %    box containing running head.
\headheight 10\p@         %
 \headsep    11\p@  %
 \footskip 23\p@
\bigskipamount=\@bls \@plus 0.3\@bls \@minus 0.3\@bls % 1/1 line
\medskipamount=0.5\bigskipamount                  % 1/2 line
\smallskipamount=0.25\bigskipamount               % 1/4 line
 \textheight 46\baselineskip
\advance\textheight\topskip
 \textwidth 32pc
                             % For two-column mode:
 \columnsep 2pc               %   Space between columns
\columnseprule \z@           %   \@Width of rule between columns.
 \footnotesep 6.65\p@
\skip\footins 12\p@ \@plus  8\p@          % Space between last line of text and
                                      % top of first footnote.
\floatsep 8\p@ \@plus 4\p@ \@minus 2\p@ % Space between adjacent floats moved
                                         % to top or bottom of text page.
\textfloatsep 8\p@ \@plus 4\p@ \@minus 2\p@ % Space between main text and floats
                                         % at top or bottom of page.
\intextsep 8\p@ \@plus 4\p@ \@minus 2\p@ % Space between in-text figures and
                                         % text.
\dblfloatsep      8\p@ \@plus 4\p@ \@minus 4\p@ % Same as \floatsep for double-column
                                         % figures in two-column mode.
\dbltextfloatsep 12\p@ \@plus 4\p@ \@minus 4\p@ % \textfloatsep for double-column
                                         % floats.
\@fptop \z@ \@plus 1fil    % Stretch at top of float page/column. (Must be
                         % \z@ \@plus ...)
\@fpsep 8\p@ \@plus 2fil    % Space between floats on float page/column.
\@fpbot \z@ \@plus 1fil    % Stretch at bottom of float page/column. (Must be
                         % \z@ \@plus ... )
\@dblfptop \z@ \@plus 1fil % Stretch at top of float page. (Must be \z@ \@plus ...)
\@dblfpsep 8\p@ \@plus 2fil % Space between floats on float page.
\@dblfpbot \z@ \@plus 1fil % Stretch at bottom of float page. (Must be
                         % \z@ \@plus ... )
\marginparpush 5\p@       % Minimum vertical separation between two marginal
                         % notes.
\parskip \z@
\parindent 1em
 \eqntopsep 8\p@ \@plus 2\p@ \@minus 2\p@ %\parskip, added above and below
\eqnarraycolsep 1\p@                 % in an \eqnarray.
\def\part{\@startsection{part}{1}{\z@}{2\@bls \@plus
  0.4\@bls}{\@bls}{\normalsize}}
\def\partmark#1{}
\if@ussrhead
\def\section{\@startsection{section}{1}{\parindent}{\@bls
 \@plus .3\@bls \@minus .1\@bls}{-0.00001pt}{\normalsize\bfseries\boldmath}{\relax}}
\else
\def\section{\@startsection{section}{1}{\z@}{\@bls
   \@plus .3\@bls \@minus .1\@bls}{\@bls}{\normalsize\bfseries
 \boldmath}}
\fi
\def\subsection{\@startsection{subsection}{2}{\z@}{\@bls
  \@plus .3\@bls \@minus .1\@bls}{\@bls}{\normalsize\itshape}}
\def\subsubsection{\@startsection{subsubsection}{3}{\z@}{\@bls
  \@plus .2\@bls}{0.0001pt}{\normalsize\itshape}}
\def\paragraph{\@startsection{paragraph}{4}{\z@}{3.25ex \@plus
  2ex \@minus 0.2ex}{-1em}{\normalfont\normalsize\itshape}}
\def\subparagraph{\@startsection{subparagraph}{5}{1em}{3.25ex \@plus
  2ex \@minus 0.2ex}{-1em}{\normalfont\normalsize\itshape}}
\def\@sect#1#2#3#4#5#6[#7]#8{%
  \ifnum #2>\c@secnumdepth
    \let\@svsec\@empty
  \else
    \refstepcounter{#1}\def\@tempa{#8}%
    \ifx\@tempa\@empty
      \edef\@svsec{\csname lb@empty@#1\endcsname}%
    \else
      \edef\@svsec{\csname lb@#1\endcsname}\fi\fi
  \@tempskipa #5\relax
  \ifdim \@tempskipa>\z@
    \begingroup #6\relax
\noindent \hskip #3\relax{\@secnumfont{#2}\@svsec}%
  {\head@style\head@format{#2}{#8}\par}%
    \endgroup
    \csname #1mark\endcsname{#7}%
    \addcontentsline{toc}{#1}{\ifnum #2>\c@secnumdepth \else
     \protect\numberline{\csname the#1\endcsname}\fi #7}%
  \else
    \def\@svsechd{#6\hskip #3\relax {\@secnumfont{#2}\@svsec }%
      {\head@style\head@format{#2}{#8}}\csname #1mark\endcsname{#7}%
    \addcontentsline{toc}{#1}{\ifnum #2>\c@secnumdepth \else
       \protect\numberline{\csname the#1\endcsname}\fi #7}}%
  \fi
  \@xsect{#5}}
\def\@ssect#1#2#3#4#5#6{%
  \@tempskipa #4\relax
  \ifdim \@tempskipa>\z@
    \begingroup
      #5{%
        \@hangfrom{\hskip #2}%
          \interlinepenalty \@M \head@format{#1}{#6}\@@par}%
    \endgroup
  \else
    \def\@svsechd{#5{\hskip #2\relax \head@format{#1}{#6}}}%
  \fi
  \@xsect{#4}}
\def\@startsection#1#2#3#4#5#6{%
  \if@noskipsec \leavevmode \fi
  \par
  \@tempskipa #4\relax
  \@afterindenttrue
  \ifdim \@tempskipa <\z@
    \@tempskipa -\@tempskipa \@afterindentfalse
  \fi
  \if@nobreak
    \everypar{}%
  \else
    \addpenalty\@secpenalty\addvspace\@tempskipa
  \fi
  \@ifstar
    {\@ssect{#2}{#3}{#4}{#5}{#6}}%
    {\@dblarg{\@sect{#1}{#2}{#3}{#4}{#5}{#6}}}}

\@maxlistdepth=2
\def\labelitemi{--}
\def\labelitemii{$\cdot$}
\def\labelenumi{(\theenumi)}        \def\theenumi{\roman{enumi}}
\def\labelenumii{(\theenumii)}      \def\theenumii{\alph{enumii}}
 \tabcolsep 1\p@           % idem in tabular environment.
\def\@arrayclassz{\ifcase \@lastchclass \@acolampacol \or \@ampacol \or
  \or \or \@addamp \or \@acolampacol \or \@firstampfalse \@acol \fi
  \edef\@preamble{\@preamble
  \ifcase \@chnum
    \hfil$\relax\displaystyle\@sharp$\hfil \or
    $\relax\displaystyle\@sharp$\hfil      \or
    \hfil$\relax\displaystyle\@sharp$\fi}}
\newdimen\Arrayrulewidth \Arrayrulewidth=1.0\p@
\def\Hline{\noalign{\ifnum0=`}\fi\hrule \@height \Arrayrulewidth \futurelet
   \@tempa\@xhline}
\newdimen\crulepostskip \crulepostskip -7\p@
\def\ccline#1{%
  \noalign{\vskip\rulepreskip}%
  \do@ccline(#1,1-0)\cr
  \noalign{\vskip\crulepostskip}}
\def\do@ccline(#1-#2,#3){%
  \ifnum#1>#2\else\@cline[#1-#2]\do@ccline(#3,1-0)\fi
  \ignorespaces}
\newdimen\@Ldec \newdimen\@Rdec
\def\dec #1.#2 {\hbox to\@Ldec{\hss#1}\def\@tempb{#2}%
\ifx\@tempb\empty\hbox to\@Rdec{\hfill}\else\hbox to\@Rdec{.#2\hss}\fi}
\def\setdec #1.#2 {\relax
  \bgroup
    \setbox0\hbox{\kern1pt\relax#1}\setbox1\hbox{\kern1pt\relax#2}%
    \global\@Ldec\wd0\global\@Rdec\wd1
  \egroup}
\setdec 000.000 % default 3 digits on each side
\newdimen\@Lpmdec \newdimen\@Rpmdec
\def\pmdec #1.#2 {\hbox to\@Lpmdec{\hss#1}\def\@tempb{#2}%
\ifx\@tempb\empty\hbox to\@Rpmdec{\hfill}\else\hbox to\@Rpmdec{.#2\hss}\fi}
\def\setpmdec #1.#2 {\relax
  \bgroup
    \setbox0\hbox{\kern1pt\relax#1}\setbox1\hbox{\kern1pt\relax#2}%
    \global\@Lpmdec\wd0\global\@Rpmdec\wd1
  \egroup}
\setpmdec 0.0 % default 1 digit on each side
\def\add@thanksref#1{%
  \global\advance\note@cnt\@ne
  \def\@tempa{*}\def\@tempb{#1}%
  \ifx\@tempa\@tempb
    \ifnum\note@cnt>\@ne \add@xtok\t@loc@notes{\note@sep}\fi
    \add@tok\t@loc@notes{*}%
  \else
    \ifnum\note@cnt>\@ne \add@xtok\t@loc@notes{\note@sep}\fi
    \add@tok\t@loc@notes{%
    \if@Elproofing#1\else\ref{#1}\fi
    }%
  \fi}
\def\thanks@optarg[#1]#2{%
  \def\@tempa{*}\def\@tempb{#1}%
  \ifx\@tempa\@tempb
    \@tempcnta=\c@footnote \c@footnote=-1 \label{#1}\c@footnote=\@tempcnta
    \gdef\@corresp@note{\footnotetext[-1]{#2}}%
  \else
    \add@tok\t@glob@notes{\footnotetext}%
    \refstepcounter{footnote}%
    \if@Elproofing\else\if@tempswa\label{#1}\else\relax\fi\fi
    \add@xtok\t@glob@notes{[\the\c@footnote]}%
    \add@tok\t@glob@notes{{#2}}%
  \fi
  \ignorespaces}
\def\frontmatter{%
  \NoHyper
  \let\@corresp@note\relax
  \global\t@glob@notes={}\global\c@author\z@
  \global\c@collab\z@ \global\c@address\z@
  \sv@mathsurround\mathsurround \m@th
  \global\n@author=0\n@author@\relax
  \global\n@collab=0\n@collab@\relax
  \global\advance\n@author\m@ne   % In comparisons later on we need
  \global\advance\n@collab\m@ne   % n@author-1 and n@collab-1
  \global\@firstauthortrue        % set to false by first \author or \collab
  \global\@hasabstractfalse       % Default:  no abstract or keywords
  \global\@haskeywordsfalse       % Default:  no abstract or keywords
  \global\@prefacefalse           %           not preface
  \ifnum\c@firstpage=\c@lastpage
    \gdef\@pagerange{\@pagenumprefix\ESpagenumber{firstpage}}
  \else
    \gdef\@pagerange{\@pagenumprefix
   \ESpagenumber{firstpage}--\@pagenumprefix\ESpagenumber{lastpage}}%
  \fi
  \@ifundefined{RIfM@}{}{\global\let\vec\@bfvec}%
  \open@fm \ignorespaces}
\def\title@fmt#1#2{%
\@ifundefined{@runtitle}{\global\def\@runtitle{#1}}{}%
 \vspace*{\@overtitleskip} % Vertical space above article type,
  \@articletypesize                  % Size for article type
  \leavevmode\vphantom{Aye!}
  \@articletype
  \vskip12\p@
  {\@titlesize #1\,\hbox{$^{#2}$}\par}%
  \vskip\@undertitleskip
  }
\def\address@fmt@init{%
  \par                                % Start new paragraph
  \vskip \@overaddressskip}               % Vertical space before addresses
\def\@abstract[#1]{%
  \global\@hasabstracttrue
  \hyphenpenalty\sv@hyphenpenalty     % restore \hyphenpenalty
  \global\setbox\t@abstract=\vbox\bgroup
  \leftskip\z@
  \@rightskip\z@ \rightskip\@rightskip \parfillskip\@flushglue
   \@abstractsize                      % Text in 9/11
  \parindent 1em                      % \parindent in abstract
  \noindent {\bfseries\abstractname}  % caption `Abstract' (bold)
  \vskip 0.5\@bls    % half a line of space below
\noindent\ignorespaces
}
\def\@bfvec#1{\boldsymbol{#1}}
\def\@pmbfvec#1{\pmb{#1}}
\mathchardef\Gamma="0000
\mathchardef\Delta="0001
\mathchardef\Theta="0002
\mathchardef\Lambda="0003
\mathchardef\Xi="0004
\mathchardef\Pi="0005
\mathchardef\Sigma="0006
\mathchardef\Upsilon="0007
\mathchardef\Phi="0008
\mathchardef\Psi="0009
\mathchardef\Omega="000A
\mathchardef\varGamma="0100
\mathchardef\varDelta="0101
\mathchardef\varTheta="0102
\mathchardef\varLambda="0103
\mathchardef\varXi="0104
\mathchardef\varPi="0105
\mathchardef\varSigma="0106
\mathchardef\varUpsilon="0107
\mathchardef\varPhi="0108
\mathchardef\varPsi="0109
\mathchardef\varOmega="010A
\if@TwoColumn
  \adjdemerits=100
  \linepenalty=100
  \doublehyphendemerits=5000        % experimental (1993-12-14)
  \emergencystretch=1.6pc
  \spaceskip=0.3em \@plus 0.17em \@minus 0.12em
\fi
\binoppenalty=300
\relpenalty=100
\clubpenalty=5000                % 'Club line'  at bottom of page.
\widowpenalty=2000               % 'Widow line' at top of page.
\displaywidowpenalty=1000        % Math display widow line.
\predisplaypenalty=150           % Breaking before a math display.
\postdisplaypenalty=50           % Breaking after a math display.
\hfuzz=1\p@
\hbadness=3000
\@frontmatterwidth\textwidth
\endinput
%%
%% End of file `elsart1p.cls'.
