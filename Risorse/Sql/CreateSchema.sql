USE [TwitterEmotions]
GO
/****** Object:  Table [dbo].[Emoticon]    Script Date: 19/04/2020 20:01:53 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Emoticon](
	[ID] int identity(1,1),
	[Code] [varchar](15) NOT NULL,
	[Polarity] [smallint] NOT NULL,
 CONSTRAINT [PK_Emoticon] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[EmoticonCount]    Script Date: 19/04/2020 20:01:53 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EmoticonCount](
	[Emotion] [varchar](15) NOT NULL,
	[IDEmoticon] int NOT NULL,
	[Count] [int] NOT NULL,
 CONSTRAINT [PK_EmoticonCount] PRIMARY KEY CLUSTERED 
(
	[Emotion] ASC,
	IDEmoticon ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[HashtagCount]    Script Date: 19/04/2020 20:01:53 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[HashtagCount](
	[Emotion] [varchar](15) NOT NULL,
	[Hashtag] [varchar](30) NOT NULL,
	[Count] [int] NOT NULL,
 CONSTRAINT [PK_HashtagCount] PRIMARY KEY CLUSTERED 
(
	[Emotion] ASC,
	[Hashtag] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[NegativeWord]    Script Date: 19/04/2020 20:01:53 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[NegativeWord](
	[Word] [varchar](15) NOT NULL,
 CONSTRAINT [PK_NegativeWord] PRIMARY KEY CLUSTERED 
(
	[Word] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Slang]    Script Date: 19/04/2020 20:01:53 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Slang](
	[Slang] [varchar](10) NOT NULL,
	[Traduction] [varchar](50) NOT NULL,
 CONSTRAINT [PK_Slang] PRIMARY KEY CLUSTERED 
(
	[Slang] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[StopWord]    Script Date: 19/04/2020 20:01:53 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[StopWord](
	[Word] [varchar](50) NOT NULL,
 CONSTRAINT [PK_StopWord] PRIMARY KEY CLUSTERED 
(
	[Word] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Tweet]    Script Date: 19/04/2020 20:01:53 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tweet](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Emotion] [varchar](15) NOT NULL,
	[Text] [varchar](1000) NOT NULL,
 CONSTRAINT [PK_Tweet] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[WordCount]    Script Date: 19/04/2020 20:01:54 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[WordCount](
	[Emotion] [varchar](15) NOT NULL,
	[Word] [varchar](30) NOT NULL,
	[Count] [int] NOT NULL,
	[FlagSentisense] [bit] NOT NULL,
	[FlagNRC] [bit] NOT NULL,
	[FlagEmoSN] [bit] NOT NULL,
 CONSTRAINT [PK_WordCount] PRIMARY KEY CLUSTERED 
(
	[Emotion] ASC,
	[Word] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[EmoticonCount]  WITH CHECK ADD  CONSTRAINT [FK_EmoticonCount_Emoticon] FOREIGN KEY([IDEmoticon])
REFERENCES [dbo].[Emoticon] ([ID])
GO
ALTER TABLE [dbo].[EmoticonCount] CHECK CONSTRAINT [FK_EmoticonCount_Emoticon]
GO
