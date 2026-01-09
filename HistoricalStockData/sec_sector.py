from edgar import set_identity
from edgar.reference.company_subsets import CompanySubset
set_identity("williamzoughaib@gmail.com")
# Get pharmaceutical companies using SIC codes
healthcare = (CompanySubset(use_comprehensive=False)
             .from_industry(sic_range=(2830, 2839))  # Drugs
             .combine_with(
                 CompanySubset(use_comprehensive=False)
                 .from_industry(sic_range=(3840, 3849))  # Medical devices
             )
             .combine_with(
                 CompanySubset(use_comprehensive=False)
                 .from_industry(sic_range=(8000, 8099))  # Health services
             )
             .get())

energy = (CompanySubset(use_comprehensive=False)
         .from_industry(sic_range=(1200, 1299))  # Coal mining
         .combine_with(CompanySubset(use_comprehensive=False)
             .from_industry(sic_range=(1300, 1399)))  # Oil & gas extraction
         .combine_with(CompanySubset(use_comprehensive=False)
             .from_industry(sic_range=(2900, 2999)))  # Petroleum refining
         .get())

materials = (CompanySubset(use_comprehensive=False)
            .from_industry(sic_range=(1000, 1099))  # Metal mining
            .combine_with(CompanySubset(use_comprehensive=False)
                .from_industry(sic_range=(1400, 1499)))  # Nonmetallic minerals
            .combine_with(CompanySubset(use_comprehensive=False)
                .from_industry(sic_range=(2600, 2699)))  # Paper
            .combine_with(CompanySubset(use_comprehensive=False)
                .from_industry(sic_range=(2800, 2829)))  # Chemicals (excluding drugs)
            .combine_with(CompanySubset(use_comprehensive=False)
                .from_industry(sic_range=(3300, 3399)))  # Primary metals
            .get())

industrials = (CompanySubset(use_comprehensive=False)
              .from_industry(sic_range=(1500, 1799))  # Construction
              .combine_with(CompanySubset(use_comprehensive=False)
                  .from_industry(sic_range=(3400, 3599)))  # Fabricated metal products, machinery
              .combine_with(CompanySubset(use_comprehensive=False)
                  .from_industry(sic_range=(3700, 3799)))  # Transportation equipment
              .combine_with(CompanySubset(use_comprehensive=False)
                  .from_industry(sic_range=(4000, 4799)))  # Transportation, communications
              .combine_with(CompanySubset(use_comprehensive=False)
                  .from_industry(sic_range=(5000, 5099)))  # Wholesale trade
              .combine_with(CompanySubset(use_comprehensive=False)
                  .from_industry(sic_range=(7200, 7399)))  # Business services
              .get())

consumer_disc = (CompanySubset(use_comprehensive=False)
                .from_industry(sic_range=(2300, 2399))  # Apparel
                .combine_with(CompanySubset(use_comprehensive=False)
                    .from_industry(sic_range=(2500, 2599)))  # Furniture
                .combine_with(CompanySubset(use_comprehensive=False)
                    .from_industry(sic_range=(3600, 3699)))  # Electronic equipment
                .combine_with(CompanySubset(use_comprehensive=False)
                    .from_industry(sic_range=(3900, 3999)))  # Miscellaneous manufacturing
                .combine_with(CompanySubset(use_comprehensive=False)
                    .from_industry(sic_range=(5200, 5999)))  # Retail trade
                .combine_with(CompanySubset(use_comprehensive=False)
                    .from_industry(sic_range=(7000, 7199)))  # Hotels, entertainment
                .get())

consumer_staples = (CompanySubset(use_comprehensive=False)
                   .from_industry(sic_range=(2000, 2099))  # Food products
                   .combine_with(CompanySubset(use_comprehensive=False)
                       .from_industry(sic_range=(2100, 2199)))  # Tobacco
                   .combine_with(CompanySubset(use_comprehensive=False)
                       .from_industry(sic_range=(5100, 5199)))  # Wholesale nondurable goods
                   .combine_with(CompanySubset(use_comprehensive=False)
                       .from_industry(sic_range=(5400, 5499)))  # Food stores
                   .get())

financials = (CompanySubset(use_comprehensive=False)
             .from_industry(sic_range=(6000, 6799))  # Banks, insurance, real estate, investment
             .get())

technology = (CompanySubset(use_comprehensive=False)
             .from_industry(sic_range=(3570, 3579))  # Computer equipment
             .combine_with(CompanySubset(use_comprehensive=False)
                 .from_industry(sic_range=(3600, 3699)))  # Electronics (overlap with discretionary)
             .combine_with(CompanySubset(use_comprehensive=False)
                 .from_industry(sic_range=(7370, 7379)))  # Computer programming, software
             .get())

communication = (CompanySubset(use_comprehensive=False)
                .from_industry(sic_range=(4800, 4899))  # Communications
                .get())

utilities = (CompanySubset(use_comprehensive=False)
            .from_industry(sic_range=(4900, 4999))  # Electric, gas, sanitary services
            .get())

real_estate = (CompanySubset(use_comprehensive=False)
              .from_industry(sic_range=(6500, 6599))  # Real estate
              .get())

