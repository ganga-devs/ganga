%define name ganga
%define version 0.1.0_rc6
%define unmangled_version 0.1.0_rc6
%define release 1

Summary: ARDA Dashboard Application
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{unmangled_version}.tar.gz
License: UNKNOWN
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: "Ganga <project-ganga-developers@cern.ch>"
Url: http://ganga.web.cern.ch/ganga/

%description
The aim of the ARDA project is to provide the LHC experiments
tools for Distributed Analysis of their data on the gLite middleware
provided by the EGEE project (Enabling Grids for E-sciencE)

%prep
%setup -n %{name}-%{unmangled_version}

%build
python setup.py build

%install
python setup.py install -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)
