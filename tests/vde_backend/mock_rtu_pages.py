LOGIN_PAGE = '''
<!DOCTYPE html>
<html>
<head>
	<title>RTU</title>

    <!--stylesheets-->
    <link rel="stylesheet" href="/rtui/lib/jqwidgets/styles/jqx.base.ef501e7c58.css" type="text/css" />
    <link rel="stylesheet" href="/rtui/app/app.min.2b64c2f7aa.css">

    <!--Libraries-->
    <script type="text/javascript" src="/rtui/lib/jQuery/jquery-2.1.4.min.f9c7afd057.js"></script>

    <script src="/rtui/lib/jqwidgets/jqwidgets.min.49e8e356c5.js"></script>

    <!-- jszip -->
    <script type="text/javascript" src="/rtui/lib/jszip/jszip.b2b9eb4084.js"></script>

    <!--framework-->
    <script type="text/javascript" src="/rtui/framework/js/lib/jed.5f120f7e32.js"></script>
    <script type="text/javascript" src="/rtui/framework/js/webui.f670dbad8f.js"></script>
    <script type="text/javascript" src="/rtui/framework/js/lib/CSSOM.40c9db686f.js"></script>

    <script src="/rtui/app/app.min.f7f387fc5e.js"></script>

    <!-- old webserver-->
    <script type="text/javascript" src="/ws/ssi/rtuAsyncReq.js" type="text/javascript"></script>
    <script type="text/javascript" src="/ws/ssi/rtuLangFile.js" type="text/javascript"></script>

</head>
<body style="margin:0;">
    <!--layout of SPA (jqxDockPanel)-->
    <div id="RTUPageLayout">
        <!--div for notifications -->
        <div id='RTU_UI_NotificationsArea'></div>
        <div id='RTU_UI_ErrorLoggerPopup'></div>
        <!--fix header on top-->
        <div id='RTU_UI_PageHeaderPane' dock='top'></div>
        <!--footer area on bottom-->
        <div id="RTU_UI_PageFooterPane" dock='bottom'></div>
        <!--tree view on left of old webserver layout-->
        <div id='RTU_UI_PageTreePane' dock='left'></div>
        <!--toggle details pane of old webserver layout-->
        <div id='RTU_UI_PageDetailsPane' dock='right'></div>
        <!-- main Content-->
        <div id='RTU_UI_PageMainContentPane' >
            <div style="font-size:8px; color:grey"> please enable javascript and/or disable compatibility mode!</div>
        </div>
    </div>
    <div class="content" id="content"></div>

    <!--script area -->
    <script type="text/javascript">

    //main document ready entry point
    // create layout

    $(document).ready(function () {
        rtu.pageLoaded();
    });

 </script>

</body>
</html>
'''
HARDWARE_PAGE = '''
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"><html>
  <head>
    <meta http-equiv="content-type" content="text/html; charset=utf-8">
    <title>httpRpmHwTree()</title>
    <base target="mainSubB">
    <link rel="stylesheet" type="text/css" href="/ws/ssi/rtuTree.css">
    <script language="JavaScript1.2" src="/ws/ssi/rtuAsyncReq.js" type="text/javascript"></script>
    <script language="JavaScript1.2" src="/ws/ssi/rtuFolder.js" type="text/javascript"></script>
    <script language="JavaScript1.2" src="/ws/ssi/rtuTree.js" type="text/javascript"></script>
  </head>
  <body>
<ul class="top">
<li>
<img src="/ws/images/minus.gif" width="9" height="9" name="RTU560" onclick="changeNode(this);">
<img src="/ws/images/rtu520.gif" width="16" height="16">RTU520:
<a href="/ABBRTU560/PrioI_Signal?ObjAddr=3">Projet ULB Coin du balais</a>
<ul>
<li>
<img src="/ws/images/minus.gif" width="9" height="9" name="Cabinet" onclick="changeNode(this);">
<img src="/ws/images/cabinet.gif" width="16" height="16">
DIN rail mounted: 1
<ul>
<li>
<img src="/ws/images/minus.gif" width="9" height="9" name="rack" onclick="changeNode(this);">
<img src="/ws/images/rack.gif" width="16" height="16">DIN rail
<ul>
<li>
<img src="/ws/images/minus.gif" width="9" height="9" name="rack" onclick="changeNode(this);">
<img src="/ws/images/rack.gif" width="16" height="16">I/O Assembly [B1.R1.A1]
<ul>
<li>
<img src="/ws/images/none.gif" width="9" height="9">
<img src="/ws/images/board.gif" width="16" height="16">Power supply device: 520PSD01
</li>
<li>
<img src="/ws/images/minus.gif" width="9" height="9" name="cmbo" onclick="changeNode(this);">
<img src="/ws/images/board.gif" width="16" height="16">CMU/AD device: <a href="/ABBRTU560/ComBoard?ID=7&TYPE=2228&RACK=0&SLOT=1">520CMD01</a> [S0]
<ul>
<li>
<img src="/ws/images/none.gif" width="9" height="9">
<img src="/ws/images/linePb.gif" width="16" height="16">WRB: I/O-Bus: 1
</li>
<li>
<img src="/ws/images/none.gif" width="9" height="9">
<img src="/ws/images/arcPrint.gif" width="16" height="16">PROCESS ARCHIVE: Archives
</li>
<li>
<img src="/ws/images/none.gif" width="9" height="9">
<img src="/ws/images/lineHost.gif" width="16" height="16"><a href="/ABBRTU560/diag/networkInterface/ui/local?IfObjAddr=24">E1</a>: Line: IEC104
</li>
<li>
<img src="/ws/images/none.gif" width="9" height="9">
<img src="/ws/images/lineHost.gif" width="16" height="16"><a href="/ABBRTU560/diag/networkInterface/ui/local?IfObjAddr=24">E1</a>: Line: MODBUS TCP
</li>
<li>
<img src="/ws/images/none.gif" width="9" height="9">
<img src="/ws/images/linePb.gif" width="16" height="16">CP2: CVI Line: 1
</li>
</ul>
</li>
</ul>
</li>
<li>
<img src="/ws/images/none.gif" width="9" height="9">
<img src="/ws/images/board.gif" width="16" height="16"><a href="/ABBRTU560/hwTree_pdInfoMon?IDNR=0&REF=549&MODE=1">560CVD03</a> [B1.D1]: TFO 1 CVD#1 <em class="ioboardiv" id="IV549"></em>
</li>
</ul>
</li>
</ul>
</li>
</ul>
</li>
</ul>
    <script language="JavaScript1.2">init();</script>
  </body>
</html>
'''
DATA_PAGE = '''
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"><html>
  <head>
    <meta http-equiv="content-type" content="text/html; charset=utf-8">
    <title>Monitoring Information</title>
    <link rel="stylesheet" type="text/css" href="/ws/ssi/rtuDefault.css">
    <script language='JavaScript1.2'>
    <!--
      function checkSupport() {
        if (navigator.appName == "Netscape") {
          var appVersion = navigator.appVersion;
          if (appVersion.indexOf("4") == 0 || appVersion.indexOf("3") == 0 ) {
            return 0;
          }
        }
        return 1;
      }
      function again() {
        var thisHref= window.location.href;
        if (checkSupport() > 0) {
          var posInStr = thisHref.indexOf("POS=");
          if (posInStr > 1) {
            thisHref = thisHref.substr(0, (posInStr - 1));
          }
          thisHref = thisHref + "&POS=" + document.body.scrollTop;
        }
        if (thisHref.indexOf("NOSESSRETRIGGER=") < 0) {
          thisHref = thisHref + "&NOSESSRETRIGGER=1"
        }
        location = thisHref;
      }
    //-->
    </script>
  </head>
  <body onLoad="javascript:window.scrollTo(0,0);">
    <span class="nowrap">
      <h2>Monitoring Information</h2>
      <h3></h3>
      <table>
        <tr>
          <td>MFI:</td>
          <td>COS PHI</td>
          <td>-0.906700</td>
          <td><span class="small">(2022-08-09, 17:25:18.694 ST<span class='red'> TIV SB</span>)</span></td></tr>
        <tr>
          <td>MFI:</td>
          <td>PUISSANCE ACTIVE</td>
          <td>30720.000000</td>
          <td><span class="small">(2022-08-09, 17:25:00.545 ST<span class='red'> TIV SB</span>)</span></td></tr>
        <tr>
          <td>MFI:</td>
          <td>PUISSANCE APPARENTE</td>
          <td>34920.000000</td>
          <td><span class="small">(2022-08-09, 17:24:59.445 ST<span class='red'> TIV SB</span>)</span></td></tr>
        <tr>
          <td>MFI:</td>
          <td>PUISSANCE REACTIVE</td>
          <td>-6120.000000</td>
          <td><span class="small">(2022-08-09, 17:22:44.478 ST<span class='red'> TIV SB</span>)</span></td></tr>
        <tr>
          <td>MFI:</td>
          <td>TENSION PHASE 1-2</td>
          <td>237.500000</td>
          <td><span class="small">(2022-08-09, 17:15:49.569 ST<span class='red'> TIV SB</span>)</span></td></tr>
        <tr>
          <td>MFI:</td>
          <td>TENSION PHASE 2-3</td>
          <td>237.229996</td>
          <td><span class="small">(2022-08-09, 17:17:23.837 ST<span class='red'> TIV SB</span>)</span></td></tr>
        <tr>
          <td>MFI:</td>
          <td>TENSION PHASE 3-1</td>
          <td>237.319992</td>
          <td><span class="small">(2022-08-09, 17:25:15.394 ST<span class='red'> TIV SB</span>)</span></td></tr>
      </table>
      <script language="JavaScript1.2">
      <!--
        window.setTimeout("again()", 5000);
      //-->
      </script>
    </span>
  </body>
</html>
'''

