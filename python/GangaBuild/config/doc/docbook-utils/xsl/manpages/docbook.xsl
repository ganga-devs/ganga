<?xml version='1.0'?>
<!-- vim:set sts=2 shiftwidth=2 syntax=sgml: -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version='1.0'>

<xsl:import href="../html/docbook.xsl"/>
<xsl:include href="synop.xsl"/>
<xsl:include href="lists.xsl"/>
<xsl:include href="xref.xsl"/>

<!-- Needed for chunker.xsl (for now): -->
<xsl:param name="chunker.output.method" select="'text'"/>
<xsl:param name="chunker.output.encoding" select="'ISO-8859-1'"/>

<xsl:output method="text"
            encoding="ISO-8859-1"
            indent="no"/>

<!--
  named templates for bold and italic. call like:

  <xsl:apply-templates mode="bold" select="node-you-want" />
-->
<xsl:template mode="bold" match="*">
  <xsl:variable name="content">
    <xsl:apply-templates/>
  </xsl:variable>
  <xsl:text>\fB</xsl:text>
  <xsl:value-of select="$content"/>
  <xsl:text>\fR</xsl:text>
</xsl:template>

<xsl:template mode="italic" match="*">
  <xsl:variable name="content">
    <xsl:apply-templates/>
  </xsl:variable>
  <xsl:text>\fI</xsl:text>
  <xsl:value-of select="$content"/>
  <xsl:text>\fR</xsl:text>
</xsl:template>

<xsl:template match="caution|important|note|tip|warning">
  <xsl:text>&#10;.RS&#10;.Sh "</xsl:text>
  <!-- capitalize word -->
  <xsl:value-of
    select="translate (substring (name(.), 1, 1), 'cintw', 'CINTW')" />
  <xsl:value-of select="substring (name(), 2)" />
  <xsl:if test="title">
    <xsl:text>: </xsl:text>
    <xsl:value-of select="title[1]"/>
  </xsl:if>
  <xsl:text>"&#10;</xsl:text>
  <xsl:apply-templates/>
  <xsl:text>&#10;.RE&#10;</xsl:text>
</xsl:template> 

<xsl:template match="refsection|refsect1">
  <xsl:choose>
    <xsl:when test="ancestor::refsection">
      <xsl:text>&#10;.SS "</xsl:text>
      <xsl:value-of select="title[1]"/>
    </xsl:when>
    <xsl:otherwise>
      <xsl:text>&#10;.SH "</xsl:text>
      <xsl:value-of select="translate(title[1],'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')"/>
    </xsl:otherwise>
  </xsl:choose>
  <xsl:text>"&#10;</xsl:text>
  <xsl:apply-templates/>
</xsl:template>

<xsl:template match="refsect2">
  <xsl:text>&#10;.SS "</xsl:text>
  <xsl:value-of select="title[1]"/>
  <xsl:text>"&#10;</xsl:text>
  <xsl:apply-templates/>
</xsl:template>

<xsl:template match="refsynopsisdiv">
  <xsl:text>&#10;.SH "SYNOPSIS"&#10;</xsl:text>
  <xsl:apply-templates/>
</xsl:template>


<xsl:template match="para">
  <xsl:text>&#10;.PP&#10;</xsl:text>
  <xsl:for-each select="node()">
    <xsl:choose>
      <xsl:when test="self::literallayout|self::informaltable|self::screen|
		      self::programlisting|self::itemizedlist|
		      self::orderedlist|self::variablelist|self::simplelist">
        <xsl:text>&#10;</xsl:text>
        <xsl:apply-templates select="."/>
      </xsl:when>
      <xsl:when test="self::text()">
	<xsl:if test="starts-with(translate(.,'&#10;',' '), ' ') and
		      preceding-sibling::node()[name(.)!='']">
	  <xsl:text> </xsl:text>
	</xsl:if>
        <xsl:variable name="content">
	  <xsl:apply-templates select="."/>
	</xsl:variable>
	<xsl:value-of select="normalize-space($content)"/>
	<xsl:if
        test="translate(substring(., string-length(.), 1),'&#10;',' ') = ' ' and
              following-sibling::node()[name(.)!='']">
	  <xsl:text> </xsl:text>
	</xsl:if>
      </xsl:when>
      <xsl:otherwise>
        <xsl:variable name="content">
          <xsl:apply-templates select="."/>
        </xsl:variable>
        <xsl:value-of select="normalize-space($content)"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:for-each>
  <xsl:text>&#10;</xsl:text>
</xsl:template>

<xsl:template match="simpara">
  <xsl:variable name="content">
    <xsl:apply-templates/>
  </xsl:variable>
  <xsl:text>&#10;&#10;</xsl:text>
  <xsl:value-of select="normalize-space($content)"/>
  <xsl:text>
</xsl:text>
</xsl:template>

  
<xsl:template match="refentry">

  <xsl:variable name="section">
    <xsl:choose>
      <xsl:when test="refmeta/manvolnum">
        <xsl:value-of select="refmeta/manvolnum[1]"/>
      </xsl:when>
      <xsl:when test=".//funcsynopsis">3</xsl:when>
      <xsl:otherwise>1</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="name" select="refnamediv/refname[1]"/>

  <!-- standard man page width is 64 chars; 6 chars needed for the two
       (x) volume numbers, and 2 spaces, leaves 56 -->
  <xsl:variable name="twidth" select="(56 - string-length(refmeta/refentrytitle)) div 2"/>

  <xsl:variable name="reftitle" 
		select="substring(refmeta/refentrytitle, 1, $twidth)"/>

  <xsl:variable name="title">
    <xsl:choose>
      <xsl:when test="refentryinfo/title">
        <xsl:value-of select="refentryinfo/title"/>
      </xsl:when>
      <xsl:when test="../referenceinfo/title">
        <xsl:value-of select="../referenceinfo/title"/>
      </xsl:when>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="date">
    <xsl:choose>
      <xsl:when test="refentryinfo/date">
        <xsl:value-of select="refentryinfo/date"/>
      </xsl:when>
      <xsl:when test="../referenceinfo/date">
        <xsl:value-of select="../referenceinfo/date"/>
      </xsl:when>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="productname">
    <xsl:choose>
      <xsl:when test="refentryinfo/productname">
        <xsl:value-of select="refentryinfo/productname"/>
      </xsl:when>
      <xsl:when test="../referenceinfo/productname">
        <xsl:value-of select="../referenceinfo/productname"/>
      </xsl:when>
    </xsl:choose>
  </xsl:variable>

  <!-- replace spaces with underscores in the filename -->
  <xsl:variable name="filename">
    <xsl:call-template name="replace-string">
      <xsl:with-param name="content"
                      select="concat(normalize-space ($name), '.', $section)"/>
      <xsl:with-param name="replace" select="' '"/>
      <xsl:with-param name="with" select="'_'"/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:call-template name="write.text.chunk">
    <xsl:with-param name="filename" select="$filename"/>
    <xsl:with-param name="content">
      <xsl:text>.\"Generated by db2man.xsl. Don't modify this, modify the source.
.de Sh \" Subsection
.br
.if t .Sp
.ne 5
.PP
\fB\\$1\fR
.PP
..
.de Sp \" Vertical space (when we can't use .PP)
.if t .sp .5v
.if n .sp
..
.de Ip \" List item
.br
.ie \\n(.$>=3 .ne \\$3
.el .ne 3
.IP "\\$1" \\$2
..
.TH "</xsl:text>
      <xsl:value-of select="translate($reftitle,'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')"/>
      <xsl:text>" </xsl:text>
      <xsl:value-of select="$section"/>
      <xsl:text> "</xsl:text>
      <xsl:value-of select="normalize-space($date)"/>
      <xsl:text>" "</xsl:text>
      <xsl:value-of select="normalize-space($productname)"/>
      <xsl:text>" "</xsl:text>
      <xsl:value-of select="$title"/>
      <xsl:text>"
</xsl:text>
      <xsl:apply-templates/>
      <xsl:text>&#10;</xsl:text>

      <!-- Author section -->
      <xsl:choose>
        <xsl:when test="refentryinfo//author">
          <xsl:apply-templates select="refentryinfo" mode="authorsect"/>
        </xsl:when>
        <xsl:when test="/book/bookinfo//author">
          <xsl:apply-templates select="/book/bookinfo" mode="authorsect"/>
        </xsl:when>
        <xsl:when test="/article/articleinfo//author">
          <xsl:apply-templates select="/article/articleinfo" mode="authorsect"/>
        </xsl:when>
      </xsl:choose>

    </xsl:with-param>
  </xsl:call-template>
  <!-- Now generate stub include pages for every page documented in
       this refentry (except the page itself) -->
  <xsl:for-each select="refnamediv/refname">
    <xsl:if test=". != $name">
      <xsl:call-template name="write.text.chunk">
	<xsl:with-param name="filename"
		        select="concat(normalize-space(.), '.', $section)"/>
	<xsl:with-param name="content" select="concat('.so man',
	      $section, '/', $name, '.', $section, '&#10;')"/>
      </xsl:call-template>
    </xsl:if>
  </xsl:for-each>
</xsl:template>

<xsl:template match="refmeta"></xsl:template>
<xsl:template match="title"></xsl:template>
<xsl:template match="abstract"></xsl:template>

<xsl:template match="articleinfo|bookinfo|refentryinfo" mode="authorsect">
  <xsl:text>.SH AUTHOR</xsl:text>
  <xsl:if test="count(.//author)>1">
    <xsl:text>S</xsl:text>
  </xsl:if>
  <xsl:text>&#10;</xsl:text>

  <xsl:for-each select=".//author">
    <xsl:if test="position() > 1">
      <xsl:text>, </xsl:text>
    </xsl:if>
    <xsl:variable name="author">
      <xsl:apply-templates select="."/>
    </xsl:variable>
    <xsl:value-of select="normalize-space($author)"/>    
  </xsl:for-each>
  <xsl:text>.&#10;</xsl:text>
  <xsl:if test=".//editor">
    <xsl:text>.br&#10;Man page edited by </xsl:text>
    <xsl:apply-templates select=".//editor"/>
    <xsl:text>.&#10;</xsl:text>
  </xsl:if>
</xsl:template>

<xsl:template match="author|editor">
  <xsl:call-template name="person.name"/>
  <xsl:apply-templates select="./affiliation/address/email" />
</xsl:template>

<xsl:template match="copyright">
  <xsl:text>Copyright \(co  </xsl:text>
  <xsl:apply-templates select="./year" />
  <xsl:text>&#10;.Sp&#10;</xsl:text>
</xsl:template>

<xsl:template match="email">
  <xsl:text> &lt;</xsl:text>
  <xsl:apply-templates/>
  <xsl:text>&gt;</xsl:text>
</xsl:template>

<xsl:template match="refnamediv">
  <xsl:text>.SH NAME&#10;</xsl:text>
  <xsl:for-each select="refname">
    <xsl:if test="position()>1">
      <xsl:text>, </xsl:text>
    </xsl:if>
    <xsl:value-of select="."/>
  </xsl:for-each>
  <xsl:text> \- </xsl:text>
  <xsl:value-of select="normalize-space (refpurpose)"/>
</xsl:template>

<xsl:template match="refentry/refentryinfo"></xsl:template>

<xsl:template match="informalexample|screen">
  <xsl:text>&#10;.IP&#10;</xsl:text>
  <xsl:apply-templates/>
</xsl:template>

<xsl:template match="filename|replaceable|varname">
  <xsl:text>\fI</xsl:text><xsl:apply-templates/><xsl:text>\fR</xsl:text>
</xsl:template>

<xsl:template match="option|userinput|envar|errorcode|constant|type">
  <xsl:text>\fB</xsl:text><xsl:apply-templates/><xsl:text>\fR</xsl:text>
</xsl:template>

<xsl:template match="emphasis">
  <xsl:choose>
    <xsl:when test="@role = 'bold' and $emphasis.propagates.style != 0">
      <xsl:text>\fB</xsl:text><xsl:apply-templates/><xsl:text>\fR</xsl:text>
    </xsl:when>
    <xsl:otherwise>
      <xsl:text>\fI</xsl:text><xsl:apply-templates/><xsl:text>\fR</xsl:text>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template match="quote">
  <xsl:text>``</xsl:text>
  <xsl:apply-templates/>
  <xsl:text>''</xsl:text>
</xsl:template>

<xsl:template match="programlisting|literallayout">
  <xsl:text>&#10;.nf&#10;</xsl:text>
  <xsl:apply-templates/>
  <xsl:text>&#10;.fi&#10;</xsl:text>
</xsl:template>

<xsl:template match="optional">
  <xsl:value-of select="$arg.choice.opt.open.str"/>
  <xsl:apply-templates/>
  <xsl:value-of select="$arg.choice.opt.close.str"/>
</xsl:template>

<xsl:template name="do-citerefentry">
  <xsl:param name="refentrytitle" select="''"/>
  <xsl:param name="manvolnum" select="''"/>

  <xsl:apply-templates mode="bold" select="$refentrytitle"/>
  <xsl:text>(</xsl:text>
  <xsl:value-of select="$manvolnum"/>
  <xsl:text>)</xsl:text>
</xsl:template>

<xsl:template match="citerefentry">
  <xsl:call-template name="do-citerefentry">
    <xsl:with-param name="refentrytitle" select="refentrytitle"/>
    <xsl:with-param name="manvolnum" select="manvolnum"/>
  </xsl:call-template>
</xsl:template>

<xsl:template match="ulink">
  <xsl:variable name="content">
    <xsl:apply-templates/>
  </xsl:variable>
  <xsl:variable name="url" select="@url"/>
  <xsl:choose>
    <xsl:when test="$url=$content or $content=''">
      <xsl:text>\fI</xsl:text>
      <xsl:value-of select="$url"/>
      <xsl:text>\fR</xsl:text>
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="$content"/>
      <xsl:text>: \fI</xsl:text>
      <xsl:value-of select="$url"/>
      <xsl:text>\fR</xsl:text>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<!-- Translate some entities to textual equivalents. -->
<xsl:template name="replace-string">
  <xsl:param name="content" select="''"/>
  <xsl:param name="replace" select="''"/>
  <xsl:param name="with" select="''"/>
  <xsl:choose>
    <xsl:when test="not(contains($content,$replace))">
      <xsl:value-of select="$content"/>
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="substring-before($content,$replace)"/>
      <xsl:value-of select="$with"/>
      <xsl:call-template name="replace-string">
        <xsl:with-param name="content"
             select="substring-after($content,$replace)"/>
        <xsl:with-param name="replace" select="$replace"/>
        <xsl:with-param name="with" select="$with"/>
      </xsl:call-template>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template name="replace-dash">
  <xsl:param name="content" select="''"/>
  <xsl:call-template name="replace-string">
    <xsl:with-param name="content" select="$content"/>
    <xsl:with-param name="replace" select="'-'"/>
    <xsl:with-param name="with" select="'\-'"/>
  </xsl:call-template>
</xsl:template>

<xsl:template name="replace-ndash">
  <xsl:param name="content" select="''"/>
  <xsl:call-template name="replace-string">
    <xsl:with-param name="content" select="$content"/>
    <xsl:with-param name="replace" select="'&#8211;'"/>
    <xsl:with-param name="with" select="'-'"/>
  </xsl:call-template>
</xsl:template>

<xsl:template name="replace-mdash">
  <xsl:param name="content" select="''"/>
  <xsl:call-template name="replace-string">
    <xsl:with-param name="content" select="$content"/>
    <xsl:with-param name="replace" select="'&#8212;'"/>
    <xsl:with-param name="with" select="'--'"/>
  </xsl:call-template>
</xsl:template>

<xsl:template name="replace-hellip">
  <xsl:param name="content" select="''"/>
  <xsl:call-template name="replace-string">
    <xsl:with-param name="content" select="$content"/>
    <xsl:with-param name="replace" select="'&#8230;'"/>
    <xsl:with-param name="with" select="'...'"/>
  </xsl:call-template>
</xsl:template>

<xsl:template name="replace-setmn">
  <xsl:param name="content" select="''"/>
  <xsl:call-template name="replace-string">
    <xsl:with-param name="content" select="$content"/>
    <xsl:with-param name="replace" select="'&#8726;'"/>
    <xsl:with-param name="with" select="'\\'"/>
  </xsl:call-template>
</xsl:template>

<xsl:template name="replace-minus">
  <xsl:param name="content" select="''"/>
  <xsl:value-of select="translate($content,'&#8722;','-')"/>
</xsl:template>

<xsl:template name="replace-nbsp">
  <xsl:param name="content" select="''"/>
  <xsl:call-template name="replace-string">
    <xsl:with-param name="content" select="$content"/>
    <xsl:with-param name="replace" select="'&#x00a0;'"/>
    <xsl:with-param name="with" select="'\~'"/>
  </xsl:call-template>
</xsl:template>

<xsl:template name="replace-backslash">
  <xsl:param name="content" select="''"/>
  <xsl:call-template name="replace-string">
    <xsl:with-param name="content" select="$content"/>
    <xsl:with-param name="replace" select="'\'"/>
    <xsl:with-param name="with" select="'\\'"/>
  </xsl:call-template>
</xsl:template>

<!-- if a period character is output at the beginning of a line
  it will be interpreted as a groff macro, so prefix all periods
  with "\&", a zero-width space. -->
<xsl:template name="replace-period">
  <xsl:param name="content" select="''"/>
  <xsl:call-template name="replace-string">
    <xsl:with-param name="content" select="$content"/>
    <xsl:with-param name="replace" select="'.'"/>
    <xsl:with-param name="with" select="'\&#38;.'"/>
  </xsl:call-template>
</xsl:template>

<xsl:template name="replace-entities">
  <xsl:param name="content" select="''"/>
  <xsl:call-template name="replace-hellip">
    <xsl:with-param name="content">
      <xsl:call-template name="replace-minus">
        <xsl:with-param name="content">
          <xsl:call-template name="replace-mdash">
            <xsl:with-param name="content">
              <xsl:call-template name="replace-ndash">
                <xsl:with-param name="content">
                  <xsl:call-template name="replace-dash">
                    <xsl:with-param name="content">
                      <xsl:call-template name="replace-setmn">
                        <xsl:with-param name="content">
        		  <xsl:call-template name="replace-period">
                            <xsl:with-param name="content">
                              <xsl:call-template name="replace-nbsp">
		                <xsl:with-param name="content">
		                  <xsl:call-template name="replace-backslash">
		                    <xsl:with-param name="content" select="$content"/>
			          </xsl:call-template>
			        </xsl:with-param>
			      </xsl:call-template>
			    </xsl:with-param>
			  </xsl:call-template>
			</xsl:with-param>
                      </xsl:call-template>
                    </xsl:with-param>
		  </xsl:call-template>
                </xsl:with-param>
              </xsl:call-template>
            </xsl:with-param>
          </xsl:call-template>
        </xsl:with-param>
      </xsl:call-template>
    </xsl:with-param>
  </xsl:call-template>
</xsl:template>

<xsl:template match="dingbat.characters">
  <!-- now that I'm using the real serializer, all that dingbat malarky -->
  <!-- isn't necessary anymore... -->
  <xsl:param name="dingbat">bullet</xsl:param>
  <xsl:choose>
    <xsl:when test="$dingbat='bullet'">\(bu</xsl:when>
    <xsl:when test="$dingbat='copyright'">\(co</xsl:when>
    <xsl:when test="$dingbat='trademark'">\(tm</xsl:when>
    <xsl:when test="$dingbat='trade'">\(tm</xsl:when>
    <xsl:when test="$dingbat='registered'">\(rg</xsl:when>
    <xsl:when test="$dingbat='service'">(SM)</xsl:when>
    <xsl:when test="$dingbat='nbsp'">\~</xsl:when>
    <xsl:when test="$dingbat='ldquo'">\(lq</xsl:when>
    <xsl:when test="$dingbat='rdquo'">\(rq</xsl:when>
    <xsl:when test="$dingbat='lsquo'">`</xsl:when>
    <xsl:when test="$dingbat='rsquo'">'</xsl:when>
    <xsl:when test="$dingbat='em-dash'">\(em</xsl:when>
    <xsl:when test="$dingbat='mdash'">\(em</xsl:when>
    <xsl:when test="$dingbat='en-dash'">\(en</xsl:when>
    <xsl:when test="$dingbat='ndash'">\(en</xsl:when>
    <xsl:otherwise>
      <xsl:text>\(bu</xsl:text>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template match="text()">
  <xsl:call-template name="replace-entities">
    <xsl:with-param name="content">
      <xsl:value-of select="."/>
    </xsl:with-param>
  </xsl:call-template>
</xsl:template>

<xsl:template match="/">
  <xsl:choose>
    <xsl:when test="//refentry">
      <xsl:apply-templates select="//refentry"/>
    </xsl:when>
    <xsl:otherwise>
      <xsl:message>No refentry elements!</xsl:message>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

</xsl:stylesheet>
