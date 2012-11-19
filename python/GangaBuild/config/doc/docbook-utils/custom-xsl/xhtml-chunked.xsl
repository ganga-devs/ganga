<?xml version='1.0'?>
<!--
Copyright (c) Members of the EGEE Collaboration. 2004.
See http://www.eu-egee.org/partners/ for details on the copyright holders.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

XSL stylesheet definition for chunked XHTML output control from a docbook article/book.

@author: Ricardo Rocha <ricardo.rocha@cern.ch>
@version: $Id: xhtml-chunked.xsl,v 1.5 2008/08/28 21:47:28 rocha Exp $
-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <xsl:import href="../xsl/xhtml/chunk.xsl"/>
    <xsl:import href="xhtml-common.xsl"/>

    <!-- Proper file naming (id attribute in sect elements -->
    <xsl:param name="use.id.as.filename">1</xsl:param>

    <!-- Set to 1 to put the first section in its own file - stays with TOC by default -->
    <xsl:param name="chunk.first.sections">0</xsl:param>

    <!-- Table of contents control -->
    <xsl:param name="generate.section.toc.level">1</xsl:param>
    <xsl:param name="toc.max.depth">3</xsl:param>
    <xsl:param name="toc.section.depth">3</xsl:param>
    
    <!-- Page Header Customization -->
    <xsl:template name="header.navigation">
        <xsl:param name="prev" select="/foo"/>
        <xsl:param name="next" select="/foo"/>
        <xsl:param name="nav.context"/>
        <xsl:variable name="home" select="/*[1]"/>
        <xsl:variable name="up" select="parent::*"/>
        <xsl:call-template name="navigation.bar">
            <xsl:with-param name="prev" select="$prev"/>
            <xsl:with-param name="next" select="$next"/>
            <xsl:with-param name="nav.context" select="$nav.context"/>
            <xsl:with-param name="home" select="$home"/>
            <xsl:with-param name="up" select="$up"/>
        </xsl:call-template>
        <hr/>
    </xsl:template>
        
    <!-- Page Footer Customization -->
    <xsl:template name="footer.navigation">
        <xsl:param name="prev" select="/foo"/>
        <xsl:param name="next" select="/foo"/>
        <xsl:param name="nav.context"/>
        <xsl:variable name="home" select="/*[1]"/>
        <xsl:variable name="up" select="parent::*"/>
        
        <hr/>
        <xsl:call-template name="navigation.bar">
            <xsl:with-param name="prev" select="$prev"/>
            <xsl:with-param name="next" select="$next"/>
            <xsl:with-param name="nav.context" select="$nav.context"/>
            <xsl:with-param name="home" select="$home"/>
            <xsl:with-param name="up" select="$up"/>
        </xsl:call-template>
    </xsl:template>

    <!-- Navigation Bar Generation -->
    <xsl:template name="navigation.bar">
        <xsl:param name="prev" select="/foo"/>
        <xsl:param name="next" select="/foo"/>
        <xsl:param name="nav.context"/>
        <xsl:param name="home"/>
        <xsl:param name="up"/>

        <table class="navbar" style="width: 100%;">
            <tr>
                <td style="width: 50%; text-align: left;">
                    <xsl:if test="count($prev)&gt;0">
                        <a>
                            <xsl:attribute name="href">
                                <xsl:call-template name="href.target">
                                    <xsl:with-param name="object" select="$prev"/>
                                </xsl:call-template>
                            </xsl:attribute>
                            <xsl:call-template name="navig.content">
                                <xsl:with-param name="direction" select="'prev'"/>
                            </xsl:call-template>
                        </a>
                        &#160;
                    </xsl:if>
                    <xsl:if test="count($home)&gt;0">
                        <a>
                            <xsl:attribute name="href">
                                <xsl:call-template name="href.target">
                                    <xsl:with-param name="object" select="$home"/>
                                </xsl:call-template>
                            </xsl:attribute>
                            <xsl:call-template name="navig.content">
                                <xsl:with-param name="direction" select="'home'"/>
                            </xsl:call-template>
                        </a>
                        &#160;
                    </xsl:if>
                    <xsl:if test="count($up)&gt;0">
                        <a>
                            <xsl:attribute name="href">
                                <xsl:call-template name="href.target">
                                    <xsl:with-param name="object" select="$up"/>
                                </xsl:call-template>
                            </xsl:attribute>
                            <xsl:call-template name="navig.content">
                                <xsl:with-param name="direction" select="'up'"/>
                            </xsl:call-template>
                        </a>
                        &#160;
                    </xsl:if>
                    <xsl:if test="count($next)&gt;0">
                        <a>
                            <xsl:attribute name="href">
                                <xsl:call-template name="href.target">
                                    <xsl:with-param name="object" select="$next"/>
                                </xsl:call-template>
                            </xsl:attribute>
                            <xsl:call-template name="navig.content">
                                <xsl:with-param name="direction" select="'next'"/>
                            </xsl:call-template>
                        </a>
                    </xsl:if>
                </td>
                <td style="width: 50%; text-align: right;">
                    <form id="search" method="GET" action="http://www.google.com/custom">
                        <input type="text" id="q" name="q" size="20" maxlength="255" value=""/>
                        &#160;
                        <input type="submit" value="Search"/>
                        <input type="hidden" name="cof" 
                            value="LW:752;L:http://diveintopython.org/images/diveintopython.png;LH:42;AH:left;GL:0;AWFID:3ced2bb1f7f1b212;"/>
                        <input type="hidden" name="domains" value="cern.ch"/>
                        <input type="hidden" name="sitesearch" value="cern.ch"/>
                    </form>
                </td>
            </tr>
        </table>
    </xsl:template>
    
        
    <xsl:template match="videodata">
    	<xsl:variable name="filename">
    		<xsl:choose>
      			<xsl:when test="local-name(.) = 'graphic' or local-name(.) = 'inlinegraphic'">
		        	<xsl:call-template name="mediaobject.filename">
        				<xsl:with-param name="object" select="."/>
        			</xsl:call-template>
      			</xsl:when>
      			<xsl:otherwise>
			        <xsl:call-template name="mediaobject.filename">
          				<xsl:with-param name="object" select=".."/>
        			</xsl:call-template>
      			</xsl:otherwise>
    		</xsl:choose>
  		</xsl:variable>
  
  		<xsl:variable name="width">
  			<xsl:choose>
  				<xsl:when test="@width != ''">
  					<xsl:value-of select="@width"/>
  				</xsl:when>
  				<xsl:otherwise>720</xsl:otherwise>
  			</xsl:choose>
  		</xsl:variable>
  		
  		<xsl:variable name="depth">
  			<xsl:choose>
  				<xsl:when test="@depth != ''">
  					<xsl:value-of select="@depth"/>
  				</xsl:when>
  				<xsl:otherwise>590</xsl:otherwise>
  			</xsl:choose>
  		</xsl:variable>
  		
  		<embed src="{$filename}" type="application/x-shockwave-flash" 
  		  width="{$width}" height="{$depth}" 
  		  allowscriptaccess="always" allowfullscreen="true"></embed>
  		  
    </xsl:template>

</xsl:stylesheet>
