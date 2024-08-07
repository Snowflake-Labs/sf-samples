USE ROLE ACCOUNTADMIN;
USE WAREHOUSE APP_WH;
USE DATABASE LLM_DEMO;
USE SCHEMA SUMMIT;
CREATE OR REPLACE TABLE METADATA (
	SERIES_ID VARCHAR,
    SERIES_TITLE VARCHAR,
    EPISODE_ID VARCHAR,
	EPISODE_TITLE VARCHAR,
	EPISODE_NUMBER INT,
	SEASON INT,
    GENRE VARCHAR
);

INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE)
VALUES
('Life’s a Glitch', 'The Pilot That Glitched', 1, 1, 'Comedy'),
('Life’s a Glitch', 'Debugging Life', 2, 1, 'Comedy'),
('Life’s a Glitch', 'Reboot Romance', 3, 1, 'Comedy'),
('Life’s a Glitch', 'The Error of Our Ways', 4, 1, 'Comedy'),
('Life’s a Glitch', '404 Not Found', 5, 1, 'Comedy'),
('Life’s a Glitch', 'The Patchwork Plan', 6, 1, 'Comedy'),
('Life’s a Glitch', 'Download Dilemma', 7, 1, 'Comedy'),
('Life’s a Glitch', 'Backup Blues', 8, 1, 'Comedy'),
('Life’s a Glitch', 'The Viral Spiral', 9, 1, 'Comedy'),
('Life’s a Glitch', 'System Crash', 10, 1, 'Comedy'),
('Life’s a Glitch', 'Upgrade Uproar', 1, 2, 'Comedy'),
('Life’s a Glitch', 'Cache Me If You Can', 2, 2, 'Comedy'),
('Life’s a Glitch', 'The Streaming Struggle', 3, 2, 'Comedy'),
('Life’s a Glitch', 'The Firewall Fiasco', 4, 2, 'Comedy'),
('Life’s a Glitch', 'Phishing Frenzy', 5, 2, 'Comedy'),
('Life’s a Glitch', 'Buggy Nights', 6, 2, 'Comedy'),
('Life’s a Glitch', 'Malware Mayhem', 7, 2, 'Comedy'),
('Life’s a Glitch', 'The Syncing Feeling', 8, 2, 'Comedy'),
('Life’s a Glitch', 'Virtual Vortex', 9, 2, 'Comedy'),
('Life’s a Glitch', 'Data Breach Breakdown', 10, 2, 'Comedy'),
('Life’s a Glitch', 'Cloudy with a Chance of Data', 1, 3, 'Comedy'),
('Life’s a Glitch', 'Router Rebellion', 2, 3, 'Comedy'),
('Life’s a Glitch', 'Surge Protector', 3, 3, 'Comedy'),
('Life’s a Glitch', 'The Great Uninstall', 4, 3, 'Comedy'),
('Life’s a Glitch', 'Wi-Fi Woes', 5, 3, 'Comedy'),
('Life’s a Glitch', 'Pixelated Plans', 6, 3, 'Comedy'),
('Life’s a Glitch', 'Streaming Wars', 7, 3, 'Comedy'),
('Life’s a Glitch', 'Battery Battle', 8, 3, 'Comedy'),
('Life’s a Glitch', 'Appocalypse', 9, 3, 'Comedy'),
('Life’s a Glitch', 'The Reset Button', 10, 3, 'Comedy'),
('Life’s a Glitch', 'Bit By Bit', 1, 4, 'Comedy'),
('Life’s a Glitch', 'The Codec Conundrum', 2, 4, 'Comedy'),
('Life’s a Glitch', 'Flash Memory Mayhem', 3, 4, 'Comedy'),
('Life’s a Glitch', 'Trojan Horseplay', 4, 4, 'Comedy'),
('Life’s a Glitch', 'The Platform Plunge', 5, 4, 'Comedy'),
('Life’s a Glitch', 'Error 404: Team Not Found', 6, 4, 'Comedy'),
('Life’s a Glitch', 'Silicon Valley Shuffle', 7, 4, 'Comedy'),
('Life’s a Glitch', 'Global Settings', 8, 4, 'Comedy'),
('Life’s a Glitch', 'The Bandwidth Brigade', 9, 4, 'Comedy'),
('Life’s a Glitch', 'The Last Update', 10, 4, 'Comedy'),
('Life’s a Glitch', 'Quantum Quirks', 1, 5, 'Comedy'),
('Life’s a Glitch', 'Algorithmic Antics', 2, 5, 'Comedy'),
('Life’s a Glitch', 'The Peripheral Predicament', 3, 5, 'Comedy'),
('Life’s a Glitch', 'Server Room Shenanigans', 4, 5, 'Comedy'),
('Life’s a Glitch', 'Hard Drive Hijinks', 5, 5, 'Comedy'),
('Life’s a Glitch', 'The Encryption Escapade', 6, 5, 'Comedy'),
('Life’s a Glitch', 'AI Awry', 7, 5, 'Comedy'),
('Life’s a Glitch', 'The Link-Up Lockdown', 8, 5, 'Comedy'),
('Life’s a Glitch', 'Hashtag Hassles', 9, 5, 'Comedy'),
('Life’s a Glitch', 'The System Shutdown', 10, 5, 'Comedy');

INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE)
VALUES
('Weekend Warriors', 'The BBQ Battle', 1, 1, 'Comedy'),
('Weekend Warriors', 'Yard Sale Shenanigans', 2, 1, 'Comedy'),
('Weekend Warriors', 'DIY Disaster', 3, 1, 'Comedy'),
('Weekend Warriors', 'Garage Band Grandstand', 4, 1, 'Comedy'),
('Weekend Warriors', 'Lawnmower Lunacy', 5, 1, 'Comedy'),
('Weekend Warriors', 'The Halloween Hijinks', 6, 1, 'Comedy'),
('Weekend Warriors', 'Thanksgiving Throwdown', 7, 1, 'Comedy'),
('Weekend Warriors', 'Christmas Chaos', 8, 1, 'Comedy'),
('Weekend Warriors', 'New Year’s Nonsense', 9, 1, 'Comedy'),
('Weekend Warriors', 'Neighborhood Watch', 10, 1, 'Comedy'),
('Weekend Warriors', 'Lawnmower Races', 1, 2, 'Comedy'),
('Weekend Warriors', 'Pool Party Pranks', 2, 2, 'Comedy'),
('Weekend Warriors', 'Easter Egg Extravaganza', 3, 2, 'Comedy'),
('Weekend Warriors', 'Spring Cleaning Craziness', 4, 2, 'Comedy'),
('Weekend Warriors', 'Fourth of July Frenzy', 5, 2, 'Comedy'),
('Weekend Warriors', 'Summer Camp Capers', 6, 2, 'Comedy'),
('Weekend Warriors', 'Backyard Camping', 7, 2, 'Comedy'),
('Weekend Warriors', 'Labor Day Labyrinth', 8, 2, 'Comedy'),
('Weekend Warriors', 'Autumn Leaves Adventure', 9, 2, 'Comedy'),
('Weekend Warriors', 'Halloween Haunt', 10, 2, 'Comedy'),
('Weekend Warriors', 'Snow Day Shenanigans', 1, 3, 'Comedy'),
('Weekend Warriors', 'Valentine’s Day Vendetta', 2, 3, 'Comedy'),
('Weekend Warriors', 'St. Patrick’s Day Parade', 3, 3, 'Comedy'),
('Weekend Warriors', 'April Fools’ Folly', 4, 3, 'Comedy'),
('Weekend Warriors', 'Spring Break Blunder', 5, 3, 'Comedy'),
('Weekend Warriors', 'Easter Egg Escapade', 6, 3, 'Comedy'),
('Weekend Warriors', 'Cinco de Mayo Mischief', 7, 3, 'Comedy'),
('Weekend Warriors', 'Memorial Day Mishap', 8, 3, 'Comedy'),
('Weekend Warriors', 'Father’s Day Fiasco', 9, 3, 'Comedy'),
('Weekend Warriors', 'Fourth of July Fireworks', 10, 3, 'Comedy'),
('Weekend Warriors', 'Back to School Scramble', 1, 4, 'Comedy'),
('Weekend Warriors', 'Labor Day Laughs', 2, 4, 'Comedy'),
('Weekend Warriors', 'Halloween Haunting', 3, 4, 'Comedy'),
('Weekend Warriors', 'Thanksgiving Turmoil', 4, 4, 'Comedy'),
('Weekend Warriors', 'Black Friday Fiasco', 5, 4, 'Comedy'),
('Weekend Warriors', 'Winter Wonderland', 6, 4, 'Comedy'),
('Weekend Warriors', 'Christmas Countdown', 7, 4, 'Comedy'),
('Weekend Warriors', 'New Year’s Eve Bash', 8, 4, 'Comedy'),
('Weekend Warriors', 'Valentine’s Day Disaster', 9, 4, 'Comedy'),
('Weekend Warriors', 'St. Patrick Shenanigans', 10, 4, 'Comedy');

INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE)
VALUES
('Desert Drifters', 'First Pour', 1, 1, 'Comedy'),
('Desert Drifters', 'Sand in the Beer', 2, 1, 'Comedy'),
('Desert Drifters', 'Tumbleweed Trouble', 3, 1, 'Comedy'),
('Desert Drifters', 'The Mysterious Stranger', 4, 1, 'Comedy'),
('Desert Drifters', 'Dusty Deals', 5, 1, 'Comedy'),
('Desert Drifters', 'Cactus Juice Concoction', 6, 1, 'Comedy'),
('Desert Drifters', 'Piano in the Pit', 7, 1, 'Comedy'),
('Desert Drifters', 'The Great Saloon Heist', 8, 1, 'Comedy'),
('Desert Drifters', 'Ghost of the Desert', 9, 1, 'Comedy'),
('Desert Drifters', 'Lizard in the Liquor', 10, 1, 'Comedy'),
('Desert Drifters', 'Sheriff’s Showdown', 11, 1, 'Comedy'),
('Desert Drifters', 'Closing Time', 12, 1, 'Comedy'),
('Desert Drifters', 'New Brews', 1, 2, 'Comedy'),
('Desert Drifters', 'The Saloon Scandal', 2, 2, 'Comedy'),
('Desert Drifters', 'Full House Frenzy', 3, 2, 'Comedy'),
('Desert Drifters', 'Whiskey Whisperer', 4, 2, 'Comedy'),
('Desert Drifters', 'The Sandstorm Lock-In', 5, 2, 'Comedy'),
('Desert Drifters', 'Prickly Pear Panic', 6, 2, 'Comedy'),
('Desert Drifters', 'Prospector’s Promise', 7, 2, 'Comedy'),
('Desert Drifters', 'Dance of the Desert', 8, 2, 'Comedy'),
('Desert Drifters', 'Camel Caravan', 9, 2, 'Comedy'),
('Desert Drifters', 'Map to Nowhere', 10, 2, 'Comedy'),
('Desert Drifters', 'Bartender’s Bet', 11, 2, 'Comedy'),
('Desert Drifters', 'Midnight at the Oasis', 12, 2, 'Comedy'),
('Desert Drifters', 'Lost Lasso', 1, 3, 'Comedy'),
('Desert Drifters', 'Heatwave Hijinks', 2, 3, 'Comedy'),
('Desert Drifters', 'The Rattlesnake Wrangler', 3, 3, 'Comedy'),
('Desert Drifters', 'Night of the Coyotes', 4, 3, 'Comedy'),
('Desert Drifters', 'Mirage Madness', 5, 3, 'Comedy'),
('Desert Drifters', 'The Gold Tooth Gambit', 6, 3, 'Comedy'),
('Desert Drifters', 'Saguaro Standoff', 7, 3, 'Comedy'),
('Desert Drifters', 'Bandit’s Booze Bash', 8, 3, 'Comedy'),
('Desert Drifters', 'The Vulture Festival', 9, 3, 'Comedy'),
('Desert Drifters', 'Prospector’s Return', 10, 3, 'Comedy'),
('Desert Drifters', 'Duel at Dawn', 11, 3, 'Comedy'),
('Desert Drifters', 'The Great Desert Race', 12, 3, 'Comedy'),
('Desert Drifters', 'Moonshine Meltdown', 1, 4, 'Comedy'),
('Desert Drifters', 'The Hangover Express', 2, 4, 'Comedy'),
('Desert Drifters', 'Cactus Costume Caper', 3, 4, 'Comedy'),
('Desert Drifters', 'Saloon Serenade', 4, 4, 'Comedy'),
('Desert Drifters', 'Outlaw’s Outage', 5, 4, 'Comedy'),
('Desert Drifters', 'Dust Devil Dance', 6, 4, 'Comedy'),
('Desert Drifters', 'Barrel of Blunders', 7, 4, 'Comedy'),
('Desert Drifters', 'Tales from the Tap', 8, 4, 'Comedy'),
('Desert Drifters', 'The Wanted Poster', 9, 4, 'Comedy'),
('Desert Drifters', 'Ghost Town Groove', 10, 4, 'Comedy'),
('Desert Drifters', 'Claim Jumper’s Claim', 11, 4, 'Comedy'),
('Desert Drifters', 'Desert Drifters’ Day Off', 12, 4, 'Comedy'),
('Desert Drifters', 'The Sheriff’s Secret', 1, 5, 'Comedy'),
('Desert Drifters', 'Bounty Hunter Blues', 2, 5, 'Comedy'),
('Desert Drifters', 'The Saloon Swap', 3, 5, 'Comedy'),
('Desert Drifters', 'Scorpion Scare', 4, 5, 'Comedy'),
('Desert Drifters', 'Oasis Oddities', 5, 5, 'Comedy'),
('Desert Drifters', 'The Poker Pretense', 6, 5, 'Comedy'),
('Desert Drifters', 'Desert Duel', 7, 5, 'Comedy'),
('Desert Drifters', 'The Boiling Point', 8, 5, 'Comedy'),
('Desert Drifters', 'The Mule’s Tale', 9, 5, 'Comedy'),
('Desert Drifters', 'The Drifter’s Duel', 10, 5, 'Comedy'),
('Desert Drifters', 'Stargazer’s Standoff', 11, 5, 'Comedy'),
('Desert Drifters', 'Last Call Before Fall', 12, 5, 'Comedy'),
('Desert Drifters', 'The Canteen Conspiracy', 1, 6, 'Comedy'),
('Desert Drifters', 'Saloons and Spoons', 2, 6, 'Comedy'),
('Desert Drifters', 'The Desert Derby', 3, 6, 'Comedy'),
('Desert Drifters', 'Whispering Winds', 4, 6, 'Comedy'),
('Desert Drifters', 'Daredevil’s Draft', 5, 6, 'Comedy'),
('Desert Drifters', 'Sands of Time', 6, 6, 'Comedy'),
('Desert Drifters', 'The Mirage Market', 7, 6, 'Comedy'),
('Desert Drifters', 'The Lucky Lizard', 8, 6, 'Comedy'),
('Desert Drifters', 'The Saloon’s Secret', 9, 6, 'Comedy'),
('Desert Drifters', 'Bizarre Bazaar', 10, 6, 'Comedy'),
('Desert Drifters', 'Twilight Tavern', 11, 6, 'Comedy'),
('Desert Drifters', 'A Toast to the Ghosts', 12, 6, 'Comedy'),
('Desert Drifters', 'Dawn of the Drafts', 1, 7, 'Comedy'),
('Desert Drifters', 'The Final Pour', 2, 7, 'Comedy'),
('Desert Drifters', 'Tales of the Tumbleweeds', 3, 7, 'Comedy'),
('Desert Drifters', 'Last Roundup', 4, 7, 'Comedy'),
('Desert Drifters', 'Saloon Sunset', 5, 7, 'Comedy'),
('Desert Drifters', 'The Showdown at High Noon', 6, 7, 'Comedy'),
('Desert Drifters', 'Desert Dreams', 7, 7, 'Comedy'),
('Desert Drifters', 'The Gold Rush Rush', 8, 7, 'Comedy'),
('Desert Drifters', 'Spirits in the Sand', 9, 7, 'Comedy'),
('Desert Drifters', 'The Saloon Farewell', 10, 7, 'Comedy'),
('Desert Drifters', 'Legend of the Lost Lager', 11, 7, 'Comedy'),
('Desert Drifters', 'The Last Call', 12, 7, 'Comedy');

INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE)
VALUES
('Tangled Ties', 'First Impressions', 1, 1, 'Drama'),
('Tangled Ties', 'Crossing Paths', 2, 1, 'Drama'),
('Tangled Ties', 'Secrets and Lies', 3, 1, 'Drama'),
('Tangled Ties', 'Promises Kept', 4, 1, 'Drama'),
('Tangled Ties', 'Office Politics', 5, 1, 'Drama'),
('Tangled Ties', 'Hidden Agendas', 6, 1, 'Drama'),
('Tangled Ties', 'Breaking Point', 7, 1, 'Drama'),
('Tangled Ties', 'Shifting Alliances', 8, 1, 'Drama'),
('Tangled Ties', 'The Reveal', 9, 1, 'Drama'),
('Tangled Ties', 'Under Pressure', 10, 1, 'Drama'),
('Tangled Ties', 'Collateral Damage', 11, 1, 'Drama'),
('Tangled Ties', 'Turning Tides', 12, 1, 'Drama'),
('Tangled Ties', 'New Beginnings', 1, 2, 'Drama'),
('Tangled Ties', 'Trust Issues', 2, 2, 'Drama'),
('Tangled Ties', 'Behind Closed Doors', 3, 2, 'Drama'),
('Tangled Ties', 'Lost Connections', 4, 2, 'Drama'),
('Tangled Ties', 'Power Plays', 5, 2, 'Drama'),
('Tangled Ties', 'Unspoken Words', 6, 2, 'Drama'),
('Tangled Ties', 'Family Matters', 7, 2, 'Drama'),
('Tangled Ties', 'Crossroads', 8, 2, 'Drama'),
('Tangled Ties', 'The Ultimatum', 9, 2, 'Drama'),
('Tangled Ties', 'Fall From Grace', 10, 2, 'Drama'),
('Tangled Ties', 'The Storm Within', 11, 2, 'Drama'),
('Tangled Ties', 'At a Crossroad', 12, 2, 'Drama'),
('Tangled Ties', 'The Aftermath', 1, 3, 'Drama'),
('Tangled Ties', 'Mending Fences', 2, 3, 'Drama'),
('Tangled Ties', 'The Aftermath', 1, 3, 'Drama'),
('Tangled Ties', 'Mending Fences', 2, 3, 'Drama'),
('Tangled Ties', 'Lines Drawn', 3, 3, 'Drama'),
('Tangled Ties', 'Shadows of Doubt', 4, 3, 'Drama'),
('Tangled Ties', 'Alliances Formed', 5, 3, 'Drama'),
('Tangled Ties', 'The Confrontation', 6, 3, 'Drama'),
('Tangled Ties', 'Under the Surface', 7, 3, 'Drama'),
('Tangled Ties', 'Fractured Lives', 8, 3, 'Drama'),
('Tangled Ties', 'Winds of Change', 9, 3, 'Drama'),
('Tangled Ties', 'The Fallout', 10, 3, 'Drama'),
('Tangled Ties', 'Reconciliation', 11, 3, 'Drama'),
('Tangled Ties', 'Ties That Bind', 12, 3, 'Drama'),
('Tangled Ties', 'Rising Tensions', 1, 4, 'Drama'),
('Tangled Ties', 'Hidden Truths', 2, 4, 'Drama'),
('Tangled Ties', 'Echoes of the Past', 3, 4, 'Drama'),
('Tangled Ties', 'Breaking Free', 4, 4, 'Drama'),
('Tangled Ties', 'Truths Unveiled', 5, 4, 'Drama'),
('Tangled Ties', 'Torn Apart', 6, 4, 'Drama'),
('Tangled Ties', 'Deeper Secrets', 7, 4, 'Drama'),
('Tangled Ties', 'The Engagement', 8, 4, 'Drama'),
('Tangled Ties', 'Paths Diverge', 9, 4, 'Drama'),
('Tangled Ties', 'Loyalties Tested', 10, 4, 'Drama'),
('Tangled Ties', 'Unraveling Threads', 11, 4, 'Drama'),
('Tangled Ties', 'Before the Storm', 12, 4, 'Drama'),
('Tangled Ties', 'Broken Bonds', 1, 5, 'Drama'),
('Tangled Ties', 'Legal Battles', 2, 5, 'Drama'),
('Tangled Ties', 'New Beginnings', 3, 5, 'Drama'),
('Tangled Ties', 'The Gathering Storm', 4, 5, 'Drama'),
('Tangled Ties', 'Silent Sacrifices', 5, 5, 'Drama'),
('Tangled Ties', 'Past and Present', 6, 5, 'Drama'),
('Tangled Ties', 'Crossing the Line', 7, 5, 'Drama'),
('Tangled Ties', 'The Final Say', 8, 5, 'Drama'),
('Tangled Ties', 'The Price of Peace', 9, 5, 'Drama'),
('Tangled Ties', 'Shattered Pieces', 10, 5, 'Drama'),
('Tangled Ties', 'Healing Wounds', 11, 5, 'Drama'),
('Tangled Ties', 'Hope on the Horizon', 12, 5, 'Drama'),
('Tangled Ties', 'Full Circle', 1, 6, 'Drama'),
('Tangled Ties', 'The Resolution', 2, 6, 'Drama'),
('Tangled Ties', 'New Challenges', 3, 6, 'Drama'),
('Tangled Ties', 'Bonds Renewed', 4, 6, 'Drama'),
('Tangled Ties', 'The Long Goodbye', 5, 6, 'Drama'),
('Tangled Ties', 'Legacy', 6, 6, 'Drama'),
('Tangled Ties', 'Second Chances', 7, 6, 'Drama'),
('Tangled Ties', 'The Edge of Tomorrow', 8, 6, 'Drama'),
('Tangled Ties', 'Forgotten Promises', 9, 6, 'Drama'),
('Tangled Ties', 'A New Day', 10, 6, 'Drama'),
('Tangled Ties', 'The Farewell', 11, 6, 'Drama'),
('Tangled Ties', 'The Journey Ahead', 12, 6, 'Drama');

INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Galactic Guardians', 'Pilot: Beyond the Stars', 1, 1, 'Action'),
('Galactic Guardians', 'Rogue Planet', 2, 1, 'Action'),
('Galactic Guardians', 'The Nebula Trap', 3, 1, 'Action'),
('Galactic Guardians', 'Echoes of Time', 4, 1, 'Action'),
('Galactic Guardians', 'The Quantum Jump', 5, 1, 'Action'),
('Galactic Guardians', 'Black Hole Shadows', 6, 1, 'Action'),
('Galactic Guardians', 'The Alien Code', 7, 1, 'Action'),
('Galactic Guardians', 'Starship Down', 8, 1, 'Action'),
('Galactic Guardians', 'The Cosmic Alliance', 9, 1, 'Action'),
('Galactic Guardians', 'The Space-Time Paradox', 10, 1, 'Action'),
('Galactic Guardians', 'Planetary Siege', 11, 1, 'Action'),
('Galactic Guardians', 'Escape from Vortex Sigma', 12, 1, 'Action'),
('Galactic Guardians', 'The Orion Mystery', 1, 2, 'Action'),
('Galactic Guardians', 'The Vanishing Moons', 2, 2, 'Action'),
('Galactic Guardians', 'Collision Course', 3, 2, 'Action'),
('Galactic Guardians', 'The Galactic Core', 4, 2, 'Action'),
('Galactic Guardians', 'Invasion of the Shadow Beasts', 5, 2, 'Action'),
('Galactic Guardians', 'The Lost Colony', 6, 2, 'Action'),
('Galactic Guardians', 'Through the Asteroid Field', 7, 2, 'Action'),
('Galactic Guardians', 'The Mind Control Crisis', 8, 2, 'Action'),
('Galactic Guardians', 'The Time Shifters', 9, 2, 'Action'),
('Galactic Guardians', 'Under the Twin Suns', 10, 2, 'Action'),
('Galactic Guardians', 'The Gravity Well', 11, 2, 'Action'),
('Galactic Guardians', 'Race Against Time', 12, 2, 'Action'),
('Galactic Guardians', 'New Horizons', 1, 3, 'Action'),
('Galactic Guardians', 'The Comets Curse', 2, 3, 'Action'),
('Galactic Guardians', 'The Stolen Artifact', 3, 3, 'Action'),
('Galactic Guardians', 'Anomaly Beyond', 4, 3, 'Action'),
('Galactic Guardians', 'The Portal Opens', 5, 3, 'Action'),
('Galactic Guardians', 'The Warlords of Draconis', 6, 3, 'Action'),
('Galactic Guardians', 'Chase Through the Cloud Nebula', 7, 3, 'Action'),
('Galactic Guardians', 'Betrayal at Orion Belt', 8, 3, 'Action'),
('Galactic Guardians', 'The Great Escape', 9, 3, 'Action'),
('Galactic Guardians', 'Return of the Ancients', 10, 3, 'Action'),
('Galactic Guardians', 'The Dark Matter Conspiracy', 11, 3, 'Action'),
('Galactic Guardians', 'Endgame', 12, 3, 'Action'),
('Galactic Guardians', 'Resurgence', 1, 4, 'Action'),
('Galactic Guardians', 'The Final Frontier', 2, 4, 'Action'),
('Galactic Guardians', 'The Crystal Moon', 3, 4, 'Action'),
('Galactic Guardians', 'The Solar Flares', 4, 4, 'Action'),
('Galactic Guardians', 'The Revolution', 5, 4, 'Action'),
('Galactic Guardians', 'The Shadow Operation', 6, 4, 'Action'),
('Galactic Guardians', 'The Galactic Summit', 7, 4, 'Action'),
('Galactic Guardians', 'The Red Giant', 8, 4, 'Action'),
('Galactic Guardians', 'The Phoenix Rises', 9, 4, 'Action'),
('Galactic Guardians', 'The Singularity Event', 10, 4, 'Action'),
('Galactic Guardians', 'The Interstellar Treaty', 11, 4, 'Action'),
('Galactic Guardians', 'Beyond the Universe', 12, 4, 'Action');

INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Houston Hospital', 'The New Surgeon', 1, 1, 'Drama'),
('Houston Hospital', 'Critical Decisions', 2, 1, 'Drama'),
('Houston Hospital', 'Midnight Emergency', 3, 1, 'Drama'),
('Houston Hospital', 'The Heart of the Matter', 4, 1, 'Drama'),
('Houston Hospital', 'Crossroads', 5, 1, 'Drama'),
('Houston Hospital', 'Under Pressure', 6, 1, 'Drama'),
('Houston Hospital', 'Family Ties', 7, 1, 'Drama'),
('Houston Hospital', 'A Dose of Reality', 8, 1, 'Drama'),
('Houston Hospital', 'Breaking Point', 9, 1, 'Drama'),
('Houston Hospital', 'The Long Shift', 10, 1, 'Drama');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Houston Hospital', 'Merge and Conquer', 1, 2, 'Drama'),
('Houston Hospital', 'New Faces, Old Problems', 2, 2, 'Drama'),
('Houston Hospital', 'Protocol Breach', 3, 2, 'Drama'),
('Houston Hospital', 'Second Chances', 4, 2, 'Drama'),
('Houston Hospital', 'The Blame Game', 5, 2, 'Drama'),
('Houston Hospital', 'Balancing Act', 6, 2, 'Drama'),
('Houston Hospital', 'Falling Through the Cracks', 7, 2, 'Drama'),
('Houston Hospital', 'The Unseen Wounds', 8, 2, 'Drama'),
('Houston Hospital', 'Turf Wars', 9, 2, 'Drama'),
('Houston Hospital', 'A New Normal', 10, 2, 'Drama');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Houston Hospital', 'Secrets Unveiled', 1, 3, 'Drama'),
('Houston Hospital', 'Past Imperfect', 2, 3, 'Drama'),
('Houston Hospital', 'Hidden Scars', 3, 3, 'Drama'),
('Houston Hospital', 'What Lies Beneath', 4, 3, 'Drama'),
('Houston Hospital', 'Forgive and Forget', 5, 3, 'Drama'),
('Houston Hospital', 'Heartstrings', 6, 3, 'Drama'),
('Houston Hospital', 'Lost Connections', 7, 3, 'Drama'),
('Houston Hospital', 'Old Flames', 8, 3, 'Drama'),
('Houston Hospital', 'Family Feud', 9, 3, 'Drama'),
('Houston Hospital', 'Crossing Lines', 10, 3, 'Drama');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Houston Hospital', 'Outbreak', 1, 4, 'Drama'),
('Houston Hospital', 'Quarantine', 2, 4, 'Drama'),
('Houston Hospital', 'Critical Mass', 3, 4, 'Drama'),
('Houston Hospital', 'The Contagion', 4, 4, 'Drama'),
('Houston Hospital', 'Isolation', 5, 4, 'Drama'),
('Houston Hospital', 'Code Zero', 6, 4, 'Drama'),
('Houston Hospital', 'Breaking Point', 7, 4, 'Drama'),
('Houston Hospital', 'The Cure', 8, 4, 'Drama'),
('Houston Hospital', 'Aftermath', 9, 4, 'Drama'),
('Houston Hospital', 'Healing Wounds', 10, 4, 'Drama');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Houston Hospital', 'The Next Wave', 1, 5, 'Drama'),
('Houston Hospital', 'Cyber Medicine', 2, 5, 'Drama'),
('Houston Hospital', 'Enhanced Reality', 3, 5, 'Drama'),
('Houston Hospital', 'Robotic Precision', 4, 5, 'Drama'),
('Houston Hospital', 'Digital Dilemmas', 5, 5, 'Drama'),
('Houston Hospital', 'AI Diagnostics', 6, 5, 'Drama'),
('Houston Hospital', 'The Human Element', 7, 5, 'Drama'),
('Houston Hospital', 'Tech Trials', 8, 5, 'Drama'),
('Houston Hospital', 'Future Tense', 9, 5, 'Drama'),
('Houston Hospital', 'Ethical Edges', 10, 5, 'Drama');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Houston Hospital', 'Budget Cuts', 1, 6, 'Drama'),
('Houston Hospital', 'The Layoff', 2, 6, 'Drama'),
('Houston Hospital', 'Underfunded', 3, 6, 'Drama'),
('Houston Hospital', 'Costly Decisions', 4, 6, 'Drama'),
('Houston Hospital', 'Penny Pinching', 5, 6, 'Drama'),
('Houston Hospital', 'Save or Sacrifice', 6, 6, 'Drama'),
('Houston Hospital', 'Economic Strain', 7, 6, 'Drama'),
('Houston Hospital', 'Downsizing', 8, 6, 'Drama'),
('Houston Hospital', 'Fundraising Efforts', 9, 6, 'Drama'),
('Houston Hospital', 'Turning Points', 10, 6, 'Drama');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Houston Hospital', 'The Lawsuit', 1, 7, 'Drama'),
('Houston Hospital', 'Courtroom Revelations', 2, 7, 'Drama'),
('Houston Hospital', 'Legal Limbo', 3, 7, 'Drama'),
('Houston Hospital', 'Testimony', 4, 7, 'Drama'),
('Houston Hospital', 'Cross Examination', 5, 7, 'Drama'),
('Houston Hospital', 'Jury Decision', 6, 7, 'Drama'),
('Houston Hospital', 'Settlement', 7, 7, 'Drama'),
('Houston Hospital', 'Appeal', 8, 7, 'Drama'),
('Houston Hospital', 'Legal Strategy', 9, 7, 'Drama'),
('Houston Hospital', 'Case Closed', 10, 7, 'Drama');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Houston Hospital', 'Experimental Treatment', 1, 8, 'Drama'),
('Houston Hospital', 'The Trial', 2, 8, 'Drama'),
('Houston Hospital', 'Patient Zero', 3, 8, 'Drama'),
('Houston Hospital', 'Therapeutic Breakthrough', 4, 8, 'Drama'),
('Houston Hospital', 'Consent Forms', 5, 8, 'Drama'),
('Houston Hospital', 'The Side Effects', 6, 8, 'Drama'),
('Houston Hospital', 'Bioethics', 7, 8, 'Drama'),
('Houston Hospital', 'Public Backlash', 8, 8, 'Drama'),
('Houston Hospital', 'Clinical Findings', 9, 8, 'Drama'),
('Houston Hospital', 'Program Review', 10, 8, 'Drama');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Houston Hospital', 'End of an Era', 1, 9, 'Drama'),
('Houston Hospital', 'Passing the Torch', 2, 9, 'Drama'),
('Houston Hospital', 'Legacy', 3, 9, 'Drama'),
('Houston Hospital', 'Farewell Speech', 4, 9, 'Drama'),
('Houston Hospital', 'Life After Medicine', 5, 9, 'Drama'),
('Houston Hospital', 'New Leadership', 6, 9, 'Drama'),
('Houston Hospital', 'The Mentor', 7, 9, 'Drama'),
('Houston Hospital', 'Reflections', 8, 9, 'Drama'),
('Houston Hospital', 'Looking Back', 9, 9, 'Drama'),
('Houston Hospital', 'New Beginnings', 10, 9, 'Drama');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Houston Hospital', 'The Last Day', 1, 10, 'Drama'),
('Houston Hospital', 'Memories', 2, 10, 'Drama'),
('Houston Hospital', 'The Final Diagnosis', 3, 10, 'Drama'),
('Houston Hospital', 'Goodbyes', 4, 10, 'Drama'),
('Houston Hospital', 'The Departure', 5, 10, 'Drama'),
('Houston Hospital', 'Closing Doors', 6, 10, 'Drama'),
('Houston Hospital', 'The Legacy Continues', 7, 10, 'Drama'),
('Houston Hospital', 'Last Rounds', 8, 10, 'Drama'),
('Houston Hospital', 'The Final Shift', 9, 10, 'Drama'),
('Houston Hospital', 'Epilogue', 10, 10, 'Drama');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('The Finest of Fort Worth', 'First Call', 1, 1, 'Drama'),
('The Finest of Fort Worth', 'The Chase Begins', 2, 1, 'Drama'),
('The Finest of Fort Worth', 'Shadows of the Past', 3, 1, 'Drama'),
('The Finest of Fort Worth', 'Undercover Blues', 4, 1, 'Drama'),
('The Finest of Fort Worth', 'High Stakes', 5, 1, 'Drama'),
('The Finest of Fort Worth', 'Breaking Point', 6, 1, 'Drama'),
('The Finest of Fort Worth', 'Crossfire', 7, 1, 'Drama'),
('The Finest of Fort Worth', 'Night Watch', 8, 1, 'Drama'),
('The Finest of Fort Worth', 'The Informant', 9, 1, 'Drama'),
('The Finest of Fort Worth', 'The Great Escape', 10, 1, 'Drama'),
('The Finest of Fort Worth', 'Hidden Agendas', 11, 1, 'Drama'),
('The Finest of Fort Worth', 'Silent Alarm', 12, 1, 'Drama'),
('The Finest of Fort Worth', 'Lives on the Line', 13, 1, 'Drama'),
('The Finest of Fort Worth', 'Code of Silence', 14, 1, 'Drama'),
('The Finest of Fort Worth', 'Truth and Consequences', 15, 1, 'Drama'),
('The Finest of Fort Worth', 'Edge of Duty', 16, 1, 'Drama'),
('The Finest of Fort Worth', 'Fallen Hero', 17, 1, 'Drama'),
('The Finest of Fort Worth', 'Beyond the Badge', 18, 1, 'Drama'),
('The Finest of Fort Worth', 'Takedown', 19, 1, 'Drama'),
('The Finest of Fort Worth', 'The Long Night', 20, 1, 'Drama'),
('The Finest of Fort Worth', 'Reckoning', 21, 1, 'Drama'),
('The Finest of Fort Worth', 'In the Line of Fire', 22, 1, 'Drama'),
('The Finest of Fort Worth', 'Sacrifices', 23, 1, 'Drama'),
('The Finest of Fort Worth', 'To Protect and Serve', 24, 1, 'Drama'),
('The Finest of Fort Worth', 'Season Finale: Judgment Day', 25, 1, 'Drama');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('The Finest of Fort Worth', 'Under Siege', 1, 2, 'Drama'),
('The Finest of Fort Worth', 'Thin Blue Line', 2, 2, 'Drama'),
('The Finest of Fort Worth', 'Mole Within', 3, 2, 'Drama'),
('The Finest of Fort Worth', 'Dirty Money', 4, 2, 'Drama'),
('The Finest of Fort Worth', 'Double Cross', 5, 2, 'Drama'),
('The Finest of Fort Worth', 'Street Justice', 6, 2, 'Drama'),
('The Finest of Fort Worth', 'Ties That Bind', 7, 2, 'Drama'),
('The Finest of Fort Worth', 'The Set Up', 8, 2, 'Drama'),
('The Finest of Fort Worth', 'Fallen Angels', 9, 2, 'Drama'),
('The Finest of Fort Worth', 'Internal Affairs', 10, 2, 'Drama'),
('The Finest of Fort Worth', 'Last Stand', 11, 2, 'Drama'),
('The Finest of Fort Worth', 'Collateral Damage', 12, 2, 'Drama'),
('The Finest of Fort Worth', 'Blind Eyes', 13, 2, 'Drama'),
('The Finest of Fort Worth', 'Echoes of Truth', 14, 2, 'Drama'),
('The Finest of Fort Worth', 'Corrupt Lines', 15, 2, 'Drama'),
('The Finest of Fort Worth', 'Breaking Cover', 16, 2, 'Drama'),
('The Finest of Fort Worth', 'Checkmate', 17, 2, 'Drama'),
('The Finest of Fort Worth', 'Undercover Unraveled', 18, 2, 'Drama'),
('The Finest of Fort Worth', 'Price of Justice', 19, 2, 'Drama'),
('The Finest of Fort Worth', 'Web of Lies', 20, 2, 'Drama'),
('The Finest of Fort Worth', 'Countdown', 21, 2, 'Drama'),
('The Finest of Fort Worth', 'Edge of Betrayal', 22, 2, 'Drama'),
('The Finest of Fort Worth', 'The Sting Operation', 23, 2, 'Drama'),
('The Finest of Fort Worth', 'Critical Blow', 24, 2, 'Drama'),
('The Finest of Fort Worth', 'Season Finale: Point of No Return', 25, 2, 'Drama');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('The Finest of Fort Worth', 'New Beginnings', 1, 3, 'Drama'),
('The Finest of Fort Worth', 'Family First', 2, 3, 'Drama'),
('The Finest of Fort Worth', 'Old Wounds', 3, 3, 'Drama'),
('The Finest of Fort Worth', 'Guardian Angels', 4, 3, 'Drama'),
('The Finest of Fort Worth', 'Sacrifices Made', 5, 3, 'Drama'),
('The Finest of Fort Worth', 'Beyond the Call', 6, 3, 'Drama'),
('The Finest of Fort Worth', 'Lives Intertwined', 7, 3, 'Drama'),
('The Finest of Fort Worth', 'Shattered Peace', 8, 3, 'Drama'),
('The Finest of Fort Worth', 'Personal Battles', 9, 3, 'Drama'),
('The Finest of Fort Worth', 'Crossroads', 10, 3, 'Drama'),
('The Finest of Fort Worth', 'The Home Front', 11, 3, 'Drama'),
('The Finest of Fort Worth', 'Turning Points', 12, 3, 'Drama'),
('The Finest of Fort Worth', 'Distant Echoes', 13, 3, 'Drama'),
('The Finest of Fort Worth', 'Closer to Home', 14, 3, 'Drama'),
('The Finest of Fort Worth', 'Unseen Sacrifices', 15, 3, 'Drama'),
('The Finest of Fort Worth', 'Retribution', 16, 3, 'Drama'),
('The Finest of Fort Worth', 'Legacy', 17, 3, 'Drama'),
('The Finest of Fort Worth', 'Endgame', 18, 3, 'Drama'),
('The Finest of Fort Worth', 'Reconciliations', 19, 3, 'Drama'),
('The Finest of Fort Worth', 'Family Matters', 20, 3, 'Drama'),
('The Finest of Fort Worth', 'The Cost of Duty', 21, 3, 'Drama'),
('The Finest of Fort Worth', 'Last Goodbyes', 22, 3, 'Drama'),
('The Finest of Fort Worth', 'Redemption', 23, 3, 'Drama'),
('The Finest of Fort Worth', 'Full Circle', 24, 3, 'Drama'),
('The Finest of Fort Worth', 'Season Finale: Resolutions', 25, 3, 'Drama');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Tennessee Track Wars', 'Start Your Engines', 1, 1, 'Action'),
('Tennessee Track Wars', 'The Qualifier', 2, 1, 'Action'),
('Tennessee Track Wars', 'Pole Position', 3, 1, 'Action'),
('Tennessee Track Wars', 'Drafting Dreams', 4, 1, 'Action'),
('Tennessee Track Wars', 'Under the Lights', 5, 1, 'Action'),
('Tennessee Track Wars', 'The Rivalry Begins', 6, 1, 'Action'),
('Tennessee Track Wars', 'Full Throttle', 7, 1, 'Action'),
('Tennessee Track Wars', 'Pit Lane Peril', 8, 1, 'Action'),
('Tennessee Track Wars', 'The Home Stretch', 9, 1, 'Action'),
('Tennessee Track Wars', 'Clash of Titans', 10, 1, 'Action'),
('Tennessee Track Wars', 'Burnout', 11, 1, 'Action'),
('Tennessee Track Wars', 'Checkered Flag', 12, 1, 'Action');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Tennessee Track Wars', 'New Challengers', 1, 2, 'Action'),
('Tennessee Track Wars', 'Speed Trap', 2, 2, 'Action'),
('Tennessee Track Wars', 'Night Race', 3, 2, 'Action'),
('Tennessee Track Wars', 'Engine Trouble', 4, 2, 'Action'),
('Tennessee Track Wars', 'Adrenaline Rush', 5, 2, 'Action'),
('Tennessee Track Wars', 'Collision Course', 6, 2, 'Action'),
('Tennessee Track Wars', 'Race Against Time', 7, 2, 'Action'),
('Tennessee Track Wars', 'Victory Lane', 8, 2, 'Action'),
('Tennessee Track Wars', 'Underdog Uprising', 9, 2, 'Action'),
('Tennessee Track Wars', 'Turbocharged', 10, 2, 'Action'),
('Tennessee Track Wars', 'Wheel to Wheel', 11, 2, 'Action'),
('Tennessee Track Wars', 'Season Finale: The Champion', 12, 2, 'Action');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Tennessee Track Wars', 'Off Track', 1, 3, 'Action'),
('Tennessee Track Wars', 'Recovery Lap', 2, 3, 'Action'),
('Tennessee Track Wars', 'Overdrive', 3, 3, 'Action'),
('Tennessee Track Wars', 'The Comeback', 4, 3, 'Action'),
('Tennessee Track Wars', 'High Gear', 5, 3, 'Action'),
('Tennessee Track Wars', 'Revved Up', 6, 3, 'Action'),
('Tennessee Track Wars', 'Speed Demon', 7, 3, 'Action'),
('Tennessee Track Wars', 'Track Rival', 8, 3, 'Action'),
('Tennessee Track Wars', 'Pole to Win', 9, 3, 'Action'),
('Tennessee Track Wars', 'The Showdown', 10, 3, 'Action'),
('Tennessee Track Wars', 'Fast Lane', 11, 3, 'Action'),
('Tennessee Track Wars', 'End of the Road', 12, 3, 'Action');
INSERT INTO METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Tennessee Track Wars', 'New Beginnings', 1, 4, 'Action'),
('Tennessee Track Wars', 'Track Legends', 2, 4, 'Action'),
('Tennessee Track Wars', 'Record Breakers', 3, 4, 'Action'),
('Tennessee Track Wars', 'Racing Heart', 4, 4, 'Action'),
('Tennessee Track Wars', 'Legacy Run', 5, 4, 'Action'),
('Tennessee Track Wars', 'Fast and Furious', 6, 4, 'Action'),
('Tennessee Track Wars', 'Beyond Speed', 7, 4, 'Action'),
('Tennessee Track Wars', 'Track King', 8, 4, 'Action'),
('Tennessee Track Wars', 'Ultimate Race', 9, 4, 'Action'),
('Tennessee Track Wars', 'Final Lap', 10, 4, 'Action'),
('Tennessee Track Wars', 'Victory Celebration', 11, 4, 'Action'),
('Tennessee Track Wars', 'Series Finale: Hall of Fame', 12, 4, 'Action');

INSERT INTO LLM_DEMO.SUMMIT.METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Legal Chronicles: LA', 'Welcome to LA Law', 1, 1, 'Drama'),
('Legal Chronicles: LA', 'The First Case', 2, 1, 'Drama'),
('Legal Chronicles: LA', 'Behind the Scenes', 3, 1, 'Drama'),
('Legal Chronicles: LA', 'Courtroom Drama', 4, 1, 'Drama'),
('Legal Chronicles: LA', 'Legal Jargon', 5, 1, 'Drama'),
('Legal Chronicles: LA', 'High Stakes', 6, 1, 'Drama'),
('Legal Chronicles: LA', 'Client Confidential', 7, 1, 'Drama'),
('Legal Chronicles: LA', 'Trial by Fire', 8, 1, 'Drama'),
('Legal Chronicles: LA', 'The Verdict', 9, 1, 'Drama'),
('Legal Chronicles: LA', 'Appeal to Reason', 10, 1, 'Drama'),
('Legal Chronicles: LA', 'Legal Ethics', 11, 1, 'Drama'),
('Legal Chronicles: LA', 'In the Public Eye', 12, 1, 'Drama'),
('Legal Chronicles: LA', 'Witness Stand', 13, 1, 'Drama'),
('Legal Chronicles: LA', 'Closing Arguments', 14, 1, 'Drama'),
('Legal Chronicles: LA', 'Justice Served', 15, 1, 'Drama'),
('Legal Chronicles: LA', 'The Aftermath', 16, 1, 'Drama'),
('Legal Chronicles: LA', 'New Beginnings', 1, 2, 'Drama'),
('Legal Chronicles: LA', 'Case Closed', 2, 2, 'Drama'),
('Legal Chronicles: LA', 'Partners in Crime', 3, 2, 'Drama'),
('Legal Chronicles: LA', 'The Insider', 4, 2, 'Drama'),
('Legal Chronicles: LA', 'Under Oath', 5, 2, 'Drama'),
('Legal Chronicles: LA', 'Judgment Day', 6, 2, 'Drama'),
('Legal Chronicles: LA', 'Family Secrets', 7, 2, 'Drama'),
('Legal Chronicles: LA', 'The Defense Rests', 8, 2, 'Drama'),
('Legal Chronicles: LA', 'Justice Delayed', 9, 2, 'Drama'),
('Legal Chronicles: LA', 'Burden of Proof', 10, 2, 'Drama'),
('Legal Chronicles: LA', 'The Long Arm of the Law', 11, 2, 'Drama'),
('Legal Chronicles: LA', 'Truth and Lies', 12, 2, 'Drama'),
('Legal Chronicles: LA', 'Cross Examination', 13, 2, 'Drama'),
('Legal Chronicles: LA', 'The Negotiation', 14, 2, 'Drama'),
('Legal Chronicles: LA', 'Mistrial', 15, 2, 'Drama'),
('Legal Chronicles: LA', 'Final Verdict', 16, 2, 'Drama'),
('Legal Chronicles: LA', 'Law and Order', 1, 3, 'Drama'),
('Legal Chronicles: LA', 'Power of Attorney', 2, 3, 'Drama'),
('Legal Chronicles: LA', 'Conflict of Interest', 3, 3, 'Drama'),
('Legal Chronicles: LA', 'Blind Justice', 4, 3, 'Drama'),
('Legal Chronicles: LA', 'The Prosecutor', 5, 3, 'Drama'),
('Legal Chronicles: LA', 'The Defense', 6, 3, 'Drama'),
('Legal Chronicles: LA', 'Habeas Corpus', 7, 3, 'Drama'),
('Legal Chronicles: LA', 'Civil Action', 8, 3, 'Drama'),
('Legal Chronicles: LA', 'The Jury', 9, 3, 'Drama'),
('Legal Chronicles: LA', 'Opening Statements', 10, 3, 'Drama'),
('Legal Chronicles: LA', 'Order in the Court', 11, 3, 'Drama'),
('Legal Chronicles: LA', 'Legal Briefs', 12, 3, 'Drama'),
('Legal Chronicles: LA', 'Discovery', 13, 3, 'Drama'),
('Legal Chronicles: LA', 'Exhibit A', 14, 3, 'Drama'),
('Legal Chronicles: LA', 'Summons', 15, 3, 'Drama'),
('Legal Chronicles: LA', 'Final Appeal', 16, 3, 'Drama'),
('Legal Chronicles: LA', 'Lawyered Up', 1, 4, 'Drama'),
('Legal Chronicles: LA', 'Case Dismissed', 2, 4, 'Drama'),
('Legal Chronicles: LA', 'The Deposition', 3, 4, 'Drama'),
('Legal Chronicles: LA', 'Pro Bono', 4, 4, 'Drama'),
('Legal Chronicles: LA', 'Closing Statements', 5, 4, 'Drama'),
('Legal Chronicles: LA', 'In Contempt', 6, 4, 'Drama'),
('Legal Chronicles: LA', 'Class Action', 7, 4, 'Drama'),
('Legal Chronicles: LA', 'Plea Bargain', 8, 4, 'Drama'),
('Legal Chronicles: LA', 'Trial Run', 9, 4, 'Drama'),
('Legal Chronicles: LA', 'Legal Eagle', 10, 4, 'Drama'),
('Legal Chronicles: LA', 'Courtroom Clash', 11, 4, 'Drama'),
('Legal Chronicles: LA', 'Crossfire', 12, 4, 'Drama'),
('Legal Chronicles: LA', 'Bench Trial', 13, 4, 'Drama'),
('Legal Chronicles: LA', 'The Litigation', 14, 4, 'Drama'),
('Legal Chronicles: LA', 'The Plea', 15, 4, 'Drama'),
('Legal Chronicles: LA', 'Legal Aid', 16, 4, 'Drama'),
('Legal Chronicles: LA', 'Opening Case', 1, 5, 'Drama'),
('Legal Chronicles: LA', 'Chambers', 2, 5, 'Drama'),
('Legal Chronicles: LA', 'Legal Battle', 3, 5, 'Drama'),
('Legal Chronicles: LA', 'Justice Served', 4, 5, 'Drama'),
('Legal Chronicles: LA', 'Court is Adjourned', 5, 5, 'Drama'),
('Legal Chronicles: LA', 'Legal Maneuvers', 6, 5, 'Drama'),
('Legal Chronicles: LA', 'The Appeal', 7, 5, 'Drama'),
('Legal Chronicles: LA', 'The Law Firm', 8, 5, 'Drama'),
('Legal Chronicles: LA', 'The Defendant', 9, 5, 'Drama'),
('Legal Chronicles: LA', 'The Prosecutors Case', 10, 5, 'Drama'),
('Legal Chronicles: LA', 'Witness Protection', 11, 5, 'Drama'),
('Legal Chronicles: LA', 'The Rebuttal', 12, 5, 'Drama'),
('Legal Chronicles: LA', 'Closing the Case', 13, 5, 'Drama'),
('Legal Chronicles: LA', 'The Ruling', 14, 5, 'Drama'),
('Legal Chronicles: LA', 'Verdict Delivered', 15, 5, 'Drama'),
('Legal Chronicles: LA', 'End of Story', 16, 5, 'Drama');

INSERT INTO LLM_DEMO.SUMMIT.METADATA (SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE) VALUES
('Pickleball Warriors', 'A Rocky Start', 1, 1, 'Sports Drama'),
('Pickleball Warriors', 'The First Match', 2, 1, 'Sports Drama'),
('Pickleball Warriors', 'Training Day', 3, 1, 'Sports Drama'),
('Pickleball Warriors', 'Team Tensions', 4, 1, 'Sports Drama'),
('Pickleball Warriors', 'New Strategies', 5, 1, 'Sports Drama'),
('Pickleball Warriors', 'Injury Time', 6, 1, 'Sports Drama'),
('Pickleball Warriors', 'Rivalries', 7, 1, 'Sports Drama'),
('Pickleball Warriors', 'Turning Point', 8, 1, 'Sports Drama'),
('Pickleball Warriors', 'The Big Game', 9, 1, 'Sports Drama'),
('Pickleball Warriors', 'After the Storm', 10, 1, 'Sports Drama'),
('Pickleball Warriors', 'Season Kickoff', 1, 2, 'Sports Drama'),
('Pickleball Warriors', 'New Recruits', 2, 2, 'Sports Drama'),
('Pickleball Warriors', 'Team Building', 3, 2, 'Sports Drama'),
('Pickleball Warriors', 'Midseason Slump', 4, 2, 'Sports Drama'),
('Pickleball Warriors', 'Fan Support', 5, 2, 'Sports Drama'),
('Pickleball Warriors', 'The Coach’s Challenge', 6, 2, 'Sports Drama'),
('Pickleball Warriors', 'Unlikely Allies', 7, 2, 'Sports Drama'),
('Pickleball Warriors', 'A New Rival', 8, 2, 'Sports Drama'),
('Pickleball Warriors', 'The Comeback', 9, 2, 'Sports Drama'),
('Pickleball Warriors', 'Championship Dreams', 10, 2, 'Sports Drama'),
('Pickleball Warriors', 'Season of Change', 1, 3, 'Sports Drama'),
('Pickleball Warriors', 'New Leadership', 2, 3, 'Sports Drama'),
('Pickleball Warriors', 'Winning Streak', 3, 3, 'Sports Drama'),
('Pickleball Warriors', 'The Downfall', 4, 3, 'Sports Drama'),
('Pickleball Warriors', 'Facing Adversity', 5, 3, 'Sports Drama'),
('Pickleball Warriors', 'The Heart of the Game', 6, 3, 'Sports Drama'),
('Pickleball Warriors', 'Community Support', 7, 3, 'Sports Drama'),
('Pickleball Warriors', 'Against All Odds', 8, 3, 'Sports Drama'),
('Pickleball Warriors', 'Semi-Finals', 9, 3, 'Sports Drama'),
('Pickleball Warriors', 'Final Showdown', 10, 3, 'Sports Drama');



UPDATE METADATA
SET SERIES_ID = CASE SERIES_TITLE
    WHEN 'Life’s a Glitch' THEN 'LG_93829372'
    WHEN 'Weekend Warriors' THEN 'WW_32729101'
    WHEN 'Desert Drifters' THEN 'DD_56201842'
    WHEN 'Tangled Ties' THEN 'TT_19380349'
    WHEN 'Galactic Guardians' THEN 'GG_29049201'
    WHEN 'Houston Hospital' THEN 'HH_83201842'
    WHEN 'The Finest of Fort Worth' THEN 'FF_74210439'
    WHEN 'Tennessee Track Wars' THEN 'TTW_31304932'
    WHEN 'Legal Chronicles: LA' THEN 'LCL72943020'
    WHEN 'Pickleball Warriors' THEN 'PW2094029'
    ELSE SERIES_ID
END;

UPDATE METADATA
SET EPISODE_ID = CONCAT('EP' || '-'  || UUID_STRING())
WHERE EPISODE_ID IS NULL;

SELECT * FROM METADATA;

CREATE OR REPLACE TABLE METADATA2 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA3 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Legal Chronicles: LA');

CREATE OR REPLACE TABLE METADATA4 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth');

CREATE OR REPLACE TABLE METADATA5 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Legal Chronicles: LA');

CREATE OR REPLACE TABLE METADATA6 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Weekend Warriors', 'Desert Drifters', 'Legal Chronicles: LA');

CREATE OR REPLACE TABLE METADATA7 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA') and EPISODE_NUMBER ='1';

CREATE OR REPLACE TABLE METADATA8 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA9 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA10 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA11 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA') and EPISODE_NUMBER ='1';

CREATE OR REPLACE TABLE METADATA12 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA') and EPISODE_NUMBER ='1';

CREATE OR REPLACE TABLE METADATA13 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA14 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA15 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA16 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA17 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA18 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA19 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA20 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA21 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA22 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA23 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA24 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Life’s a Glitch', 'Weekend Warriors', 'Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA25 AS
SELECT *
FROM METADATA
WHERE SERIES_TITLE IN ('Desert Drifters', 'Galactic Guardians', 'Houston Hospital','The Finest of Fort Worth', 'Legal Chronicles: LA', 'Pickleball Warriors');

CREATE OR REPLACE TABLE METADATA AS
SELECT * FROM metadata
UNION ALL
SELECT * FROM metadata2
UNION ALL
SELECT * FROM metadata3
UNION ALL
SELECT * FROM metadata4
UNION ALL
SELECT * FROM metadata5
UNION ALL
SELECT * FROM metadata6
UNION ALL
SELECT * FROM metadata7
UNION ALL
SELECT * FROM metadata8
UNION ALL
SELECT * FROM metadata9
UNION ALL
SELECT * FROM metadata10
UNION ALL
SELECT * FROM metadata11
UNION ALL
SELECT * FROM metadata12
UNION ALL
SELECT * FROM metadata13
UNION ALL
SELECT * FROM metadata14
UNION ALL
SELECT * FROM metadata15
UNION ALL
SELECT * FROM metadata16
UNION ALL
SELECT * FROM metadata17
UNION ALL
SELECT * FROM metadata18
UNION ALL
SELECT * FROM metadata19
UNION ALL
SELECT * FROM metadata20
UNION ALL
SELECT * FROM metadata21
UNION ALL
SELECT * FROM metadata22
UNION ALL
SELECT * FROM metadata23
UNION ALL
SELECT * FROM metadata24
UNION ALL
SELECT * FROM metadata25
;

DROP TABLE IF EXISTS metadata2;
DROP TABLE IF EXISTS metadata3;
DROP TABLE IF EXISTS metadata4;
DROP TABLE IF EXISTS metadata5;
DROP TABLE IF EXISTS metadata6;
DROP TABLE IF EXISTS metadata7;
DROP TABLE IF EXISTS metadata8;
DROP TABLE IF EXISTS metadata9;
DROP TABLE IF EXISTS metadata10;
DROP TABLE IF EXISTS metadata11;
DROP TABLE IF EXISTS metadata12;
DROP TABLE IF EXISTS metadata13;
DROP TABLE IF EXISTS metadata14;
DROP TABLE IF EXISTS metadata15;
DROP TABLE IF EXISTS metadata16;
DROP TABLE IF EXISTS metadata17;
DROP TABLE IF EXISTS metadata18;
DROP TABLE IF EXISTS metadata19;
DROP TABLE IF EXISTS metadata20;
DROP TABLE IF EXISTS metadata21;
DROP TABLE IF EXISTS metadata22;
DROP TABLE IF EXISTS metadata23;
DROP TABLE IF EXISTS metadata24;
DROP TABLE IF EXISTS metadata25;
