from django.db import models


class FxPartner(models.Model):
    """
    Currency conversion from all money's of bookmakers defined on 
    enumerator `CurrencyFixedIncome` to all partners moneys defined on 
    enumerator `CurrencyPartner`.

    # Fields
    ## From EUR
    - fx_eur_cop: `FloatField`
        - equivalent value of one Euro in Colombian pesos
    - fx_eur_mxn: `FloatField`
        - equivalent value of one Euro in Mexican Pesos
    - fx_eur_usd: `FloatField`
        - equivalent value of one Euro in US Dollars
    - fx_eur_brl: `FloatField`
        - equivalent value of one Euro in Brazilian Real
    - fx_eur_pen: `FloatField`
        - equivalent value of one Euro in Peruvian Sol
    - fx_eur_gbp: `FloatField`
        - equivalent value of one Euro in British pound
    - fx_eur_clp: `FloatField`
        - equivalent value of one Euro in Chilean peso

    ## From USD
    - fx_usd_cop: `FloatField`
        - equivalent value of one US Dollar in Colombian pesos
    - fx_usd_mxn: `FloatField`
        - equivalent value of one US Dollars in Mexican Pesos
    - fx_usd_eur: `FloatField`
        - equivalent value of one US Dollars in Euro
    - fx_usd_brl: `FloatField`
        - equivalent value of one US Dollars in Brazilian Real
    - fx_usd_pen: `FloatField`
        - equivalent value of one US Dollars in Peruvian Sol
    - fx_usd_gbp: `FloatField`
        - equivalent value of one US Dollars in British pound
    - fx_usd_clp: `FloatField`
        - equivalent value of one US Dollars in Chilean peso

    ## From COP
    - fx_cop_usd: `FloatField`
        - equivalent value of one Colombian pesos in US Dollar
    - fx_cop_mxn: `FloatField`
        - equivalent value of one Colombian pesos in Mexican Pesos
    - fx_cop_eur: `FloatField`
        - equivalent value of one Colombian pesos in Euro
    - fx_cop_brl: `FloatField`
        - equivalent value of one Colombian pesos in Brazilian Real
    - fx_cop_pen: `FloatField`
        - equivalent value of one Colombian pesos in Peruvian Sol
    - fx_cop_gbp: `FloatField`
        - equivalent value of one Colombian pesos in British pound
    - fx_cop_clp: `FloatField`
        - equivalent value of one Colombian pesos in Chilean peso

    ## From MXN
    - fx_mxn_usd: `FloatField`
        - equivalent value of one Mexican Peso in US Dollar
    - fx_mxn_cop: `FloatField`
        - equivalent value of one Mexican Peso in Colombian pesos
    - fx_mxn_eur: `FloatField`
        - equivalent value of one Mexican Peso in Euro
    - fx_mxn_brl: `FloatField`
        - equivalent value of one Mexican Peso in Brazilian Real
    - fx_mxn_pen: `FloatField`
        - equivalent value of one Mexican Peso in Peruvian Sol
    - fx_mxn_gbp: `FloatField`
        - equivalent value of one Mexican Peso in British pound
    - fx_mxn_clp: `FloatField`
        - equivalent value of one Mexican Peso in Chilean peso

    ## From GBP
    - fx_gbp_usd: `FloatField`
        - equivalent value of one British pound sterling in US Dollar
    - fx_gbp_cop: `FloatField`
        - equivalent value of one British pound sterling in Colombian pesos
    - fx_gbp_mxn: `FloatField`
        - equivalent value of one British pound sterling in Mexican Peso
    - fx_gbp_eur: `FloatField`
        - equivalent value of one British pound sterling in Euro
    - fx_gbp_brl: `FloatField`
        - equivalent value of one British pound sterling in Brazilian Real
    - fx_gbp_pen: `FloatField`
        - equivalent value of one British pound sterling in Peruvian Sol
    - fx_gbp_clp: `FloatField`
        - equivalent value of one British pound sterling in Chilean peso

    ## From PEN
    - fx_pen_usd: `FloatField`
        - equivalent value of one Peruvian Sol sterling in US Dollar
    - fx_pen_cop: `FloatField`
        - equivalent value of one Peruvian Sol sterling in Colombian pesos
    - fx_pen_mxn: `FloatField`
        - equivalent value of one Peruvian Sol sterling in Mexican Peso
    - fx_pen_eur: `FloatField`
        - equivalent value of one Peruvian Sol sterling in Euro
    - fx_pen_brl: `FloatField`
        - equivalent value of one Peruvian Sol sterling in Brazilian Real
    - fx_pen_gbp: `FloatField`
        - equivalent value of one Peruvian Sol sterling in British pound
    - fx_pen_clp: `FloatField`
        - equivalent value of one Peruvian Sol sterling in Chilean peso

    ## From CLP
    - fx_clp_usd: `FloatField`
        - equivalent value of one Chilean peso sterling in US Dollar
    - fx_clp_cop: `FloatField`
        - equivalent value of one Chilean peso sterling in Colombian pesos
    - fx_clp_mxn: `FloatField`
        - equivalent value of one Chilean peso sterling in Mexican Peso
    - fx_clp_eur: `FloatField`
        - equivalent value of one Chilean peso sterling in Euro
    - fx_clp_brl: `FloatField`
        - equivalent value of one Chilean peso sterling in Brazilian Real
    - fx_clp_pen: `FloatField`
        - equivalent value of one Chilean peso sterling in Peruvian Sol
    - fx_clp_gbp: `FloatField`
        - equivalent value of one Chilean peso sterling in British pound
    """
    # From EUR
    fx_eur_cop = models.FloatField(default=0)
    fx_eur_mxn = models.FloatField(default=0)
    fx_eur_usd = models.FloatField(default=0)
    fx_eur_brl = models.FloatField(default=0)
    fx_eur_pen = models.FloatField(default=0)
    fx_eur_gbp = models.FloatField(default=0)
    fx_eur_clp = models.FloatField(default=0)

    # From USD
    fx_usd_cop = models.FloatField(default=0)
    fx_usd_mxn = models.FloatField(default=0)
    fx_usd_eur = models.FloatField(default=0)
    fx_usd_brl = models.FloatField(default=0)
    fx_usd_pen = models.FloatField(default=0)
    fx_usd_gbp = models.FloatField(default=0)
    fx_usd_clp = models.FloatField(default=0)

    # From COP
    fx_cop_usd = models.FloatField(default=0)
    fx_cop_mxn = models.FloatField(default=0)
    fx_cop_eur = models.FloatField(default=0)
    fx_cop_brl = models.FloatField(default=0)
    fx_cop_pen = models.FloatField(default=0)
    fx_cop_gbp = models.FloatField(default=0)
    fx_cop_clp = models.FloatField(default=0)

    # From MXN
    fx_mxn_usd = models.FloatField(default=0)
    fx_mxn_cop = models.FloatField(default=0)
    fx_mxn_eur = models.FloatField(default=0)
    fx_mxn_brl = models.FloatField(default=0)
    fx_mxn_pen = models.FloatField(default=0)
    fx_mxn_gbp = models.FloatField(default=0)
    fx_mxn_clp = models.FloatField(default=0)

    # From GBP
    fx_gbp_usd = models.FloatField(default=0)
    fx_gbp_cop = models.FloatField(default=0)
    fx_gbp_mxn = models.FloatField(default=0)
    fx_gbp_eur = models.FloatField(default=0)
    fx_gbp_brl = models.FloatField(default=0)
    fx_gbp_pen = models.FloatField(default=0)
    fx_gbp_clp = models.FloatField(default=0)

    # From PEN
    fx_pen_usd = models.FloatField(default=0)
    fx_pen_cop = models.FloatField(default=0)
    fx_pen_mxn = models.FloatField(default=0)
    fx_pen_eur = models.FloatField(default=0)
    fx_pen_brl = models.FloatField(default=0)
    fx_pen_gbp = models.FloatField(default=0)
    fx_pen_clp = models.FloatField(default=0)

    # From CLP
    fx_clp_usd = models.FloatField(default=0)
    fx_clp_cop = models.FloatField(default=0)
    fx_clp_mxn = models.FloatField(default=0)
    fx_clp_eur = models.FloatField(default=0)
    fx_clp_brl = models.FloatField(default=0)
    fx_clp_gbp = models.FloatField(default=0)
    fx_clp_pen = models.FloatField(default=0)

    # From BRL
    fx_brl_usd = models.FloatField(default=0)
    fx_brl_cop = models.FloatField(default=0)
    fx_brl_mxn = models.FloatField(default=0)
    fx_brl_eur = models.FloatField(default=0)
    fx_brl_gbp = models.FloatField(default=0)
    fx_brl_pen = models.FloatField(default=0)
    fx_brl_clp = models.FloatField(default=0)

    fx_percentage = models.FloatField(default=0.95)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Fx Partner"
        verbose_name_plural = "Fx Partners"

    def __str__(self):
        return f"currency cop: {self.fx_usd_cop} - created at: {self.created_at}"
