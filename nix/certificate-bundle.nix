{ cacert, fetchurl }:

(cacert.override {
  blacklist = [ "Comodo AAA Services root" ];
}).overrideAttrs
  {
    # NSS 3.131.1 already removed emSign ECC Root CA - G3.
    version = "3.131.1";
    src = fetchurl {
      url = "https://raw.githubusercontent.com/nss-dev/nss/refs/tags/NSS_3_131_1_RTM/lib/ckfw/builtins/certdata.txt";
      hash = "sha256-PsK00kDcEsdH6vRQBh24SvdXxIO72jAvninOtdcL5RA=";
    };
  }
