<?xml version='1.0'?>
<!--
Copyright (c) Members of the EGEE Collaboration. 2004.
See http://www.eu-egee.org/partners/ for details on the copyright holders.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

XSL stylesheet definition for common XHTML output control from a docbook article/book.

@author: Ricardo Rocha <ricardo.rocha@cern.ch>
@version: $Id: xhtml-common.xsl,v 1.4 2007/02/02 15:19:02 rocha Exp $
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <!-- Contact in HTML HEAD -->
    <xsl:param name="link.mailto.url">mailto:arda-dashboard-dev@cern.ch</xsl:param>

    <!-- Set the CSS file to be used -->
    <xsl:param name="html.stylesheet">../../../common/html/css/dashboard.css</xsl:param>

    <!-- We want section numbering -->
    <xsl:param name="section.autolabel">1</xsl:param>

    <!-- Admon using graphics -->
    <xsl:param name="admon.graphics">1</xsl:param>
    <xsl:param name="admon.graphics.path">../../../common/html/images/</xsl:param>
    <xsl:param name="admon.graphics.extension">.png</xsl:param>

    <!-- Enable navigational icons -->
    <xsl:param name="navig.graphics">0</xsl:param>
    <xsl:param name="navig.graphics.path">../../../common/html/images/</xsl:param>
    <xsl:param name="navig.graphics.extension">.png</xsl:param>
    <xsl:param name="navig.showtitles">1</xsl:param>

    <!-- Callout graphics control -->
    <xsl:param name="callout.graphics.path">../../../common/html/images/callouts/</xsl:param>
    <xsl:param name="callout.graphics.extension">.png</xsl:param>

    <!-- Disable HTML BODY attributes -->
    <xsl:template name="body.attributes"/>

    <!-- Customize the TOC -->
    <xsl:param name="generate.toc">
        appendix  nop
        article   toc,title,figure,table,example,equation
        qandadiv  nop
        qandaset  toc 
        reference toc
        section   toc
    </xsl:param>

    <!-- Segmented lists as tabular lists -->
    <xsl:param name="segmentedlist.as.table" select="1"/>

</xsl:stylesheet>
